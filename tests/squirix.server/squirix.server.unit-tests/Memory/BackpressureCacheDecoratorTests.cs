using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Covers per-client isolation through <see cref="BackpressureCacheDecorator{T}" />.</summary>
public sealed class BackpressureCacheDecoratorTests : ServerUnitTestBase
{
    /// <summary>Two client ids keep independent PerClientMaxInFlight budgets.</summary>
    [Fact]
    public async Task TwoClientIdsApplyIndependentPerClientLimits()
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
            });

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Get, "jwt:client-a", DefaultCancellationToken)).Lease;

        var inner = new CompletingLogicalCache();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, new FixedClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, new FixedClientIdResolver("jwt:client-b"));

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException, NodeCacheValueResult<string>>(cacheA.GetValueAsync("c", "k", DefaultCancellationToken));
        Assert.Equal(SquirixErrorCode.TooManyRequests, rejected.Code);

        var otherClient = await cacheB.GetValueAsync("c", "k", DefaultCancellationToken);
        Assert.False(otherClient.Found);
        Assert.Equal(1, inner.GetValueCalls);
    }

    /// <summary>The void write path enforces the same per-client budgets independently across client ids.</summary>
    [Fact]
    public async Task WritePathAppliesIndependentPerClientLimits()
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
            });

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Set, "jwt:client-a", DefaultCancellationToken)).Lease;

        var inner = new CompletingLogicalCache();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, new FixedClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, new FixedClientIdResolver("jwt:client-b"));
        var entry = new NodeCacheEntry<string>("value");

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cacheA.SetEntryAsync("op-1", "c", "k", entry, DefaultCancellationToken));
        Assert.Equal(SquirixErrorCode.TooManyRequests, rejected.Code);
        Assert.Equal(0, inner.SetEntryCalls);

        await cacheB.SetEntryAsync("op-2", "c", "k", entry, DefaultCancellationToken);
        Assert.Equal(1, inner.SetEntryCalls);
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
            _ = cacheName;
            _ = key;
            _ = cancellationToken;
            _ = Interlocked.Increment(ref _getValueCalls);
            return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
        }

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<string>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cacheName;
            _ = key;
            _ = entry;
            _ = cancellationToken;
            _ = Interlocked.Increment(ref _setEntryCalls);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private sealed class FixedClientIdResolver : IBackpressureClientIdResolver
    {
        private readonly string _clientId;

        internal FixedClientIdResolver(string clientId)
        {
            _clientId = clientId;
        }

        public string Resolve() => _clientId;
    }
}
