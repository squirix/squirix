using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Verifies that cache routing uses the topology's ordinal node identity semantics.</summary>
public sealed class ClusteredCacheTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "key";
    private const string Self = "node-a";

    /// <summary>Exact owner identities execute the mutation through the local cache.</summary>
    [Fact]
    public async Task SetEntryAsyncExactOwnerUsesLocalCache()
    {
        var local = new RecordingCache();
        await using var clients = new ThrowingClientPool();
        var cache = CreateCache(Self, local, clients);

        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string> { Value = "value" }, DefaultCancellationToken);

        Assert.Equal(1, local.SetEntryCalls);
        Assert.Equal(0, clients.ForNodeCalls);
    }

    /// <summary>Owners differing only by case are remote because node identifiers are ordinal.</summary>
    [Fact]
    public async Task SetEntryAsyncCasedOwnerUsesRemoteCache()
    {
        var local = new RecordingCache();
        await using var clients = new ThrowingClientPool();
        var cache = CreateCache("NODE-A", local, clients);

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string> { Value = "value" }, DefaultCancellationToken));

        Assert.Equal(ThrowingClientPool.RemoteCallMessage, exception.Message);
        Assert.Equal(0, local.SetEntryCalls);
        Assert.Equal(1, clients.ForNodeCalls);
    }

    private static ClusteredCache<string> CreateCache(string owner, RecordingCache local, IServerClientPool clients) =>
        new(Self, local, new FixedOwnerLocator(owner), clients);

    private sealed class RecordingCache : ILogicalNamespacedCache<string>
    {
        internal int SetEntryCalls { get; private set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<string>?>(null);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<string>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            SetEntryCalls++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);
    }

    private sealed class ThrowingClientPool : IServerClientPool
    {
        internal const string RemoteCallMessage = "The remote cache path was selected.";

        internal int ForNodeCalls { get; private set; }

        public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId)
        {
            ForNodeCalls++;
            throw new InvalidOperationException(RemoteCallMessage);
        }

        public IServerCallPolicy PolicyFor(string nodeId) => throw new InvalidOperationException(RemoteCallMessage);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
