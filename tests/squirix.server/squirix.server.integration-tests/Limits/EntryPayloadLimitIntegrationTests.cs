using System;
using System.Globalization;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Limits;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.Limits;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Limits;

/// <summary>Integration coverage for fixed entry payload size limits (issue #2).</summary>
public sealed class EntryPayloadLimitIntegrationTests : IntegrationTestBase
{
    private const string NodeId = "node-payload-limit";

    /// <summary>Verifies cluster forwarding preserves ResourceExhausted when the remote owner rejects an oversized entry.</summary>
    [Fact]
    public async Task ClusterForwardPreservesPayloadTooLargeForRemoteOwner()
    {
        var urlA = GetNextHttpUri();
        var urlB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", urlA), ("node-b", urlB)]);

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "payload-limit");
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            clientA.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);

        using var channelB = CreateGrpcChannel(urlB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.False(getResponse.Found);
    }

    /// <summary>Verifies gRPC insert above the limit returns ResourceExhausted and does not persist.</summary>
    [Fact]
    public async Task GrpcInsertAboveLimitReturnsResourceExhaustedAndDoesNotPersist()
    {
        var url = GetNextHttpUri();
        await using var node = await StartNodeAsync(url, NodeId);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "grpc-over-limit",
                    Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Contains(SquirixEntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Status.Detail, StringComparison.Ordinal);

        var getResponse = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-over-limit" }, cancellationToken: DefaultCancellationToken);
        Assert.False(getResponse.Found);
    }
}
