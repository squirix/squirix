using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for the accounting of a physical insert racing a replace in <see cref="MemoryAdmissionCacheDecorator{T}" /> (issue 732).</summary>
[Immutable]
public sealed class AdmissionCacheDecoratorInsertRaceTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// Ensures a SetAsync that accounts its replace while the winning insert has reached the physical cache but is not accounted yet leaves
    /// one accounted entry: the insert and the replace both claim the key in the accounting map before they count it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InsertAndReplaceAccountOneEntry(CancellationToken cancellationToken)
    {
        const string key = "set-insert-race";
        var physical = new PhysicalCache<string>();
        var pausing = new PausingTryAddInner(new ClientCache<string>(physical, physical));
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var cache = new MemoryAdmissionCacheDecorator<string>(
            pausing,
            CreatePermissiveGate(accounting),
            estimator,
            accounting,
            RocksDoubles.CreateOwnerLocator(Self),
            Self);
        var entry = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var expectedBytes = estimator.EstimateBytes(new CacheKey(CacheName, key), entry, false);

        // The winner inserted the entry physically and stops before it accounts the insert.
        var winner = cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken).AsTask();
        await pausing.Added.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        // The other set finds the entry and accounts its replace completely while the winner is stopped.
        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken);
        pausing.Resume();
        await winner;

        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(expectedBytes);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private PressureGate CreatePermissiveGate(IMemoryUsageAccounting accounting)
    {
        var options = Options.Create(
            new PressureOptions
            {
                MaxEstimatedCacheBytes = 10_000_000_000,
                HighPressureThresholdPercent = 80,
                CriticalPressureThresholdPercent = 95,
            });
        return new PressureGate(new StateEvaluator(options), accounting, Self, _testMeter);
    }

    /// <summary>Inner cache that stops the first successful insert after it reached the physical cache until the test resumes it.</summary>
    [ThreadSafe]
    private sealed class PausingTryAddInner : ILogicalNamespacedCache<string>
    {
        private readonly TaskCompletionSource _added = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ILogicalNamespacedCache<string> _inner;
        private readonly TaskCompletionSource _resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _paused;

        internal PausingTryAddInner(ILogicalNamespacedCache<string> inner)
        {
            _inner = inner;
        }

        internal Task Added => _added.Task;

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

        public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            var added = await _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken).ConfigureAwait(false);
            if (added && Interlocked.Exchange(ref _paused, 1) == 0)
            {
                _ = _added.TrySetResult();
                await _resume.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return added;
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);

        internal void Resume() => _ = _resume.TrySetResult();
    }
}
