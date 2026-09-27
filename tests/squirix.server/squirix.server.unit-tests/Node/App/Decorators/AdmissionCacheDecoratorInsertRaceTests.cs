using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
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

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Unit tests for the accounting of a physical insert racing another write of the same key in <see cref="MemoryAdmissionCacheDecorator{T}" /> (issue 732).</summary>
[Immutable]
public sealed class AdmissionCacheDecoratorInsertRaceTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// Ensures a set of a differently sized entry that runs while the winning insert has reached the physical cache but is not accounted yet
    /// leaves one accounted entry whose size is the size of the entry the physical cache ends up holding: each key's writes reach the physical
    /// cache and the accounting in the same order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RacingWritesAccountStoredEntry(CancellationToken cancellationToken)
    {
        const string key = "set-insert-race";
        var physical = new PhysicalCache<string>();
        var real = new ClientCache<string>(physical, physical);
        var added = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var otherStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var inner = CreateStoppingInner(real, added, resume, otherStarted);
        var accounting = new MemoryUsageAccounting();
        var estimator = new CacheEntrySizeEstimator<string>();
        var cache = new MemoryAdmissionCacheDecorator<string>(
            inner,
            CreatePermissiveGate(accounting),
            estimator,
            accounting,
            RocksDoubles.CreateOwnerLocator(Self),
            Self);
        var small = new NodeCacheEntry<string> { Value = "v", Version = 1 };
        var large = new NodeCacheEntry<string> { Value = "a much longer value than the winner stores", Version = 1 };

        // The winner inserted its entry physically and stops before it accounts the insert.
        var winner = SetAsync(cache, key, small, cancellationToken);
        await added.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        // The other set writes the larger entry. Without a write gate per key it reaches the inner cache and finishes while the winner is stopped,
        // so the test waits for it to finish before the winner resumes. With a gate it waits for the winner and never reaches the inner cache, so
        // the wait for that checkpoint is bounded: on a slow machine it can only miss the interleaving, never fail the gated path.
        var other = SetAsync(cache, key, large, cancellationToken);
        var reached = await Task.WhenAny(otherStarted.Task, Task.Delay(TimeSpan.FromMilliseconds(200), TimeProvider.System, cancellationToken));
        if (reached == otherStarted.Task)
            await other;

        _ = resume.TrySetResult();
        await winner;
        await other;

        var stored = await real.GetEntryAsync(CacheName, key, cancellationToken);
        _ = await Assert.That(stored?.Value).IsEqualTo(large.Value);
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
        _ = await Assert.That(accounting.ReadEstimatedBytes()).IsEqualTo(estimator.EstimateBytes(new CacheKey(CacheName, key), large, false));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    /// <summary>Mocks the inner cache over a real one: the first successful insert stops after it reached the physical cache.</summary>
    /// <param name="real">The cache the mock forwards to.</param>
    /// <param name="added">Completed when the first insert reached the physical cache and stopped.</param>
    /// <param name="resume">Releases the stopped insert.</param>
    /// <param name="otherStarted">Completed when a second write starts reading the entry, that is when it reached the inner cache.</param>
    /// <returns>The mocked inner cache.</returns>
    private static ILogicalNamespacedCache<string> CreateStoppingInner(
        ClientCache<string> real,
        TaskCompletionSource added,
        TaskCompletionSource resume,
        TaskCompletionSource otherStarted)
    {
        var paused = 0;
        var reads = 0;
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();

        // The second read is the competing set reaching the inner cache: the winner's set read first.
        _ = inner.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                 .Callback((cacheName, key, token) =>
                  {
                      if (Interlocked.Increment(ref reads) == 2)
                          _ = otherStarted.TrySetResult();

                      return real.GetEntryAsync(cacheName, key, token);
                  }).ExpectedCallCount(2);
        _ = inner.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback(real.SetEntryAsync).ExpectedCallCount(1);
        _ = inner.Setups.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback(async (operationId, cacheName, key, entry, token) =>
                  {
                      var inserted = await real.TryAddEntryAsync(operationId, cacheName, key, entry, token).ConfigureAwait(false);
                      if (!inserted || Interlocked.Exchange(ref paused, 1) != 0)
                          return inserted;
                      _ = added.TrySetResult();
                      await resume.Task.WaitAsync(token).ConfigureAwait(false);

                      return inserted;
                  }).ExpectedCallCount(1);

        return inner.Instance();
    }

    private static Task SetAsync(MemoryAdmissionCacheDecorator<string> cache, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
        cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, key, entry, cancellationToken).AsTask();

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
}
