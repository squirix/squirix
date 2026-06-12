using System;
using System.Globalization;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Limits;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.Limits;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Limits;

/// <summary>
/// Integration coverage for fixed entry payload size limits (issue #2).
/// </summary>
public sealed class EntryPayloadLimitIntegrationTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies cluster forwarding preserves ResourceExhausted when the remote owner rejects an oversized entry.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ClusterForwardPreservesPayloadTooLargeForRemoteOwner()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = BuildClusterPeers(("node-a", urlA), ("node-b", urlB));

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "payload-limit");
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await clientA.TrySetAsync(
                new TrySetRequest
                {
                    CacheName = "default",
                    Key = key,
                    Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);

        using var channelB = CreateGrpcChannel(urlB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getEx = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await clientB.GetAsync(new GetRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.NotFound, getEx.StatusCode);
    }

    /// <summary>
    /// Verifies gRPC insert above the limit returns ResourceExhausted and does not persist.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInsertAboveLimitReturnsResourceExhaustedAndDoesNotPersist()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.SetAsync(
                new SetRequest
                {
                    CacheName = "default",
                    Key = "grpc-over-limit",
                    Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Contains(SquirixEntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Status.Detail, StringComparison.Ordinal);

        var getEx = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "grpc-over-limit" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.NotFound, getEx.StatusCode);
    }
}
