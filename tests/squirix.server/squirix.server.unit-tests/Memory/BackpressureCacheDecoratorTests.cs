using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Covers per-client isolation through <see cref="BackpressureCacheDecorator{T}" />.</summary>
[Immutable]
public sealed class BackpressureCacheDecoratorTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Two client ids keep independent PerClientMaxInFlight budgets.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoClientIdsGetIndependentLimits(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(50),
            },
            new BackpressureMetrics(_testMeter));

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Get, "jwt:client-a", cancellationToken)).Lease;

        var inner = new CompletingLogicalCache();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException, NodeCacheValueResult<string>>(cacheA.GetValueAsync("c", "k", cancellationToken));
        _ = await Assert.That(rejected.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);

        var otherClient = await cacheB.GetValueAsync("c", "k", cancellationToken);
        _ = await Assert.That(otherClient.Found).IsFalse();
        _ = await Assert.That(inner.GetValueCalls).IsEqualTo(1);
    }

    /// <summary>The void write path enforces the same per-client budgets independently across client ids.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesApplyIndependentPerClientCaps(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                RejectThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(50),
            },
            new BackpressureMetrics(_testMeter));

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Set, "jwt:client-a", cancellationToken)).Lease;

        var inner = new CompletingLogicalCache();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));
        var entry = new NodeCacheEntry<string>("value");

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cacheA.SetEntryAsync("op-1", "c", "k", entry, cancellationToken));
        _ = await Assert.That(rejected.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(inner.SetEntryCalls).IsEqualTo(0);

        await cacheB.SetEntryAsync("op-2", "c", "k", entry, cancellationToken);
        _ = await Assert.That(inner.SetEntryCalls).IsEqualTo(1);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static IBackpressureClientIdResolver CreateClientIdResolver(string clientId)
    {
        var expectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = expectations.Setups.Resolve().ReturnValue(clientId);
        return expectations.Instance();
    }

    private sealed class CompletingLogicalCache : ILogicalNamespacedCache<string>
    {
        private int _getValueCalls;
        private int _setEntryCalls;

        internal int GetValueCalls => Volatile.Read(ref _getValueCalls);

        internal int SetEntryCalls => Volatile.Read(ref _setEntryCalls);

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<string>?>(null);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _getValueCalls);
            return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
        }

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<string>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _setEntryCalls);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }
}
