using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
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

    /// <summary>A pool disposal racing remote execution surfaces Unavailable instead of ObjectDisposedException.</summary>
    [Fact]
    public async Task DisposedPolicyMapsToUnavailable()
    {
        using var meter = new Meter("test-clustered-cache-disposed");
        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter), new ServerRpcTimeoutMetrics(meter));
        var policy = new ServerCallPolicy(instrumentation, peer: "node-b");
        await policy.DisposeAsync();

        var peers = new ServerPeer[] { new() { NodeId = "node-b", Uri = new Uri("https://localhost:6500") } };
        await using var pool = new ServerClientPool(peers, new ServerClientPoolArgs { PolicyFactory = _ => policy }, new ServerClientPoolMetrics(meter));
        var cache = new ClusteredCache<string>(Self, new RecordingCache(), RocksDoubles.CreateOwnerLocator("node-b"), pool);

        var exception = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<string>?>(cache.GetEntryAsync(CacheName, Key, DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, exception.StatusCode);
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

    private static ClusteredCache<string> CreateCache(string owner, RecordingCache local, IServerClientPool clients) =>
        new(Self, local, RocksDoubles.CreateOwnerLocator(owner), clients);

    private sealed class RecordingCache : ILogicalNamespacedCache<string>
    {
        internal int SetEntryCalls { get; private set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<string>?>(null);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<string>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            SetEntryCalls++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private sealed class ThrowingClientPool : IServerClientPool
    {
        internal const string RemoteCallMessage = "The remote cache path was selected.";

        internal int ForNodeCalls { get; private set; }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId)
        {
            ForNodeCalls++;
            throw new InvalidOperationException(RemoteCallMessage);
        }

        public GrpcChannel OpenChannel(string nodeId) => throw new InvalidOperationException(RemoteCallMessage);

        public IServerCallPolicy PolicyFor(string nodeId) => throw new InvalidOperationException(RemoteCallMessage);
    }
}
