using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>On a replicated host the replica group is the only authority that dedupes a write: the node-wide replay store takes no part.</summary>
public sealed class GroupIdempotencyAuthorityTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string Owner = "node-a";
    private const int Duplicates = 8;

    /// <summary>A replicated host resolves the group coordinator, a single-copy host the one backed by the node-wide store.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostsResolveTheirOwnAuthority(CancellationToken cancellationToken)
    {
        await using var single = await StartClusterAsync("n1", new IntegrationStartOptions { EnableReplication = false }, cancellationToken);
        await using var replicated = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-composition"), cancellationToken);

        _ = await Assert.That(single["n1"].GetRequiredService<IRpcMutationIdempotencyCoordinator>()).IsTypeOf<RpcMutationIdempotencyCoordinator>();
        _ = await Assert.That(replicated[Owner].GetRequiredService<IRpcMutationIdempotencyCoordinator>()).IsTypeOf<ReplicatedMutationCoordinator>();
    }

    /// <summary>A retry of a committed write replays the group outcome and appends nothing, and the node-wide store stays empty.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryReplaysGroupOutcome(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-retry"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = AddRequest(RpcOperationIdentity.New(), cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "first");

        var first = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(retried.Added).IsTrue();
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
        _ = await Assert.That(cluster[Owner].GetRequiredService<RpcMutationIdempotencyStore>().RecordCount).IsEqualTo(0);
    }

    /// <summary>Concurrent duplicates of one write join the first commit: the group log gets one entry and every caller gets its outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentDuplicatesCommitOnce(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-concurrent"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = AddRequest(RpcOperationIdentity.New(), cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "first");
        var before = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var calls = new Task<TryAddAsyncResponse>[Duplicates];
        for (var i = 0; i < calls.Length; i++)
            calls[i] = client.TryAddEntryAsync(request, cancellationToken: cancellationToken).ResponseAsync;

        var responses = await Task.WhenAll(calls);

        foreach (var response in responses)
            _ = await Assert.That(response.Added).IsTrue();

        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(before + 1);
        _ = await Assert.That(cluster[Owner].GetRequiredService<RpcMutationIdempotencyStore>().RecordCount).IsEqualTo(0);
    }

    /// <summary>An operation identifier reused with another request is refused by the group as a reuse, and nothing is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReuseIsRefusedByGroup(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-mismatch"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var operationId = RpcOperationIdentity.New();
        var key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner);
        _ = await client.TryAddEntryAsync(AddRequest(operationId, key, "a"), cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var reused = AddRequest(operationId, key, "b");
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.TryAddEntryAsync(reused, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(refused.Status.Detail).IsEqualTo(ServerOpIdMismatchException.StableDetail);
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>A retried get-or-add answers with the first outcome, the stored entry, and appends once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddRetryReplaysOutcome(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-get-or-add"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetOrAddAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner),
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var first = await client.GetOrAddAsync(request, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.GetOrAddAsync(request, cancellationToken: cancellationToken);

        var next = request.Clone();
        next.OperationId = RpcOperationIdentity.New();
        next.Entry = new NodeCacheEntry<object?> { Value = "second", Version = 1 }.MapToProto();
        var existing = await client.GetOrAddAsync(next, cancellationToken: cancellationToken);

        _ = await Assert.That(retried).IsEqualTo(first);
        _ = await Assert.That(existing.Added).IsFalse();
        _ = await Assert.That(existing.Found).IsTrue();
        _ = await Assert.That(existing.Value).IsEqualTo(ServerProtoEx.CacheValueToGrpcValue("first"));
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
        _ = await Assert.That(cluster[Owner].GetRequiredService<RpcMutationIdempotencyStore>().RecordCount).IsEqualTo(0);
    }

    /// <summary>A decorator of the cache pipeline sees the retry of a committed write, while the group replays the outcome and appends nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DecoratorSeesRetryWhileGroupReplays(CancellationToken cancellationToken)
    {
        var touches = new int[1];
        var options = new IntegrationStartOptions
        {
            ReplicaCount = 3,
            UsePersistence = true,
            CleanTestDir = true,
            ExtraScope = "group-authority-decorator",
            Extensions = new ExtensionOptions { DecorateCachePipeline = (_, inner) => new TouchCountingPipeline(inner, touches) },
        };
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", options, cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner);
        _ = await client.TryAddEntryAsync(AddRequest(RpcOperationIdentity.New(), key, "value"), cancellationToken: cancellationToken);
        var touch = new TouchAsyncRequest { OperationId = RpcOperationIdentity.New(), CacheName = CacheName, Key = key, Expiration = Duration.FromTimeSpan(TimeSpan.FromHours(1)) };

        var first = await client.TouchAsync(touch, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.TouchAsync(touch, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Found).IsTrue();
        _ = await Assert.That(retried.Found).IsTrue();
        _ = await Assert.That(Volatile.Read(ref touches[0])).IsEqualTo(2);
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>The group tells an operation by its group and cache: the same identifier on another cache is a separate operation, not a reuse.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SameIdOnOtherCacheIsSeparate(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-scope"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var operationId = RpcOperationIdentity.New();
        var before = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var first = await client.TryAddEntryAsync(AddRequest(operationId, cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "a"), cancellationToken: cancellationToken);
        var other = AddRequest(operationId, cluster[Owner].FindKeyOwnedBy("other", Owner), "b");
        other.CacheName = "other";
        var second = await client.TryAddEntryAsync(other, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(second.Added).IsTrue();
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(before + 2);
    }

    private static IntegrationStartOptions Options(string scope) => new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = scope };

    private static TryAddEntryAsyncRequest AddRequest(string operationId, string key, string value) => new()
    {
        OperationId = operationId,
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
    };

    private static async Task<ulong> LastLogIndexAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        _ = node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Owner, out var log);
        return (await log!.GetStatusAsync(cancellationToken)).LastLogIndex;
    }

    /// <summary>Counts the touches that reach the decorated pipeline and passes every call on.</summary>
    private sealed class TouchCountingPipeline : ISquirixServerEntryCachePipeline<object?>
    {
        private readonly ISquirixServerEntryCachePipeline<object?> _inner;
        private readonly int[] _touches;

        internal TouchCountingPipeline(ISquirixServerCachePipeline inner, int[] touches)
        {
            _inner = ThrowHelper.Required(inner as ISquirixServerEntryCachePipeline<object?>, "The basic pipeline is entry-aware.");
            _touches = touches;
        }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _touches[0]);
            return _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);
        }

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) =>
            _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }
}
