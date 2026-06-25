using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Reliability;

/// <summary>Integration coverage for client operation_id propagation across cluster routing hops.</summary>
public sealed class CrossNodeOperationIdIdempotencyIntegrationTests : IntegrationTestBase
{
    private const string MismatchOperationId = "fedcba9876543210fedcba9876543210";
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Verifies bootstrap-style endpoint failover preserves operation_id and replays the cached mutation outcome.</summary>
    [Fact]
    public async Task BootstrapEndpointSwitchReplaysSameOperationId()
    {
        var urlA = GetNextHttpUri();
        var urlB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", urlA), ("node-b", urlB)]);

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-a", "bootstrap-idempotency");
        var request = new SetEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = key,
            Entry = new CacheEntry<object?> { Value = "bootstrap-value", Version = 1 }.MapToProto(),
        };

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        _ = await clientA.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);

        using var channelB = CreateGrpcChannel(urlB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        _ = await clientB.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
        Assert.Equal("bootstrap-value", (await ProtoEx.CacheValueFromGrpcValueAsync<object?>(getResponse.Value, null, null)).Value);
    }

    /// <summary>Verifies a retry with the same operation_id on a different entry node replays the owner outcome instead of double-applying when the key is owned elsewhere.</summary>
    [Fact]
    public async Task CrossNodeIdenticalOperationIdReplaysCachedResponse()
    {
        var urlA = GetNextHttpUri();
        var urlB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", urlA), ("node-b", urlB)]);

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "cross-node-idempotency");
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = key,
            Entry = new CacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var first = await clientA.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
        Assert.True(first.Added);

        using var channelB = CreateGrpcChannel(urlB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var second = await clientB.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);

        Assert.True(second.Added);

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
        Assert.Equal("first", (await ProtoEx.CacheValueFromGrpcValueAsync<object?>(getResponse.Value, null, null)).Value);
    }

    /// <summary>Verifies reusing an operation_id with a different fingerprint fails on the owner after entry-node forwarding.</summary>
    [Fact]
    public async Task CrossNodeReusedOperationIdWithDifferentFingerprintReturnsFailedPrecondition()
    {
        var urlA = GetNextHttpUri();
        var urlB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", urlA), ("node-b", urlB)]);

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var keyA = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-a");
        var keyB = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-b");

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);

        _ = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = MismatchOperationId,
                CacheName = "default",
                Key = keyA,
                Entry = new CacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            clientA.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = MismatchOperationId,
                    CacheName = "default",
                    Key = keyB,
                    Entry = new CacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal(OperationIdReuseMismatchException.StableDetail, ex.Status.Detail);
    }
}
