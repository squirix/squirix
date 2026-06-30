using System;
using System.Globalization;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for fixed entry payload size limits (issue #2).</summary>
public sealed class EntryPayloadLimitIntegrationTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-payload-limit";

    /// <summary>Verifies cluster forwarding preserves ResourceExhausted when the remote owner rejects an oversized entry.</summary>
    [Fact]
    public async Task ClusterForwardPreservesPayloadTooLargeForRemoteOwner()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers);
        await using var nodeB = await StartNodeAsync(uriB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "payload-limit");
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            clientA.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = Guid.NewGuid().ToString("N"),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);

        using var channelB = CreateGrpcChannel(uriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.False(getResponse.Found);
    }

    /// <summary>Verifies gRPC insert above the limit returns ResourceExhausted and does not persist.</summary>
    [Fact]
    public async Task GrpcInsertAboveLimitReturnsResourceExhaustedAndDoesNotPersist()
    {
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = Guid.NewGuid().ToString("N"),
                    CacheName = "default",
                    Key = "grpc-over-limit",
                    Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Contains(EntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Status.Detail, StringComparison.Ordinal);

        var getResponse = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-over-limit" }, cancellationToken: DefaultCancellationToken);
        Assert.False(getResponse.Found);
    }

    /// <summary>Verifies cluster forwarding preserves ResourceExhausted when a remote owner rejects an oversized update.</summary>
    [Fact]
    public async Task ClusterForwardPreservesPayloadTooLargeForRemoteOwnerUpdate()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers);
        await using var nodeB = await StartNodeAsync(uriB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "payload-limit-update");

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);

        _ = await clientA.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "small", Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            clientA.UpdateAsync(
                new UpdateAsyncRequest
                {
                    OperationId = Guid.NewGuid().ToString("N"),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = value }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);

        var getResponse = await clientA.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
        Assert.Equal("small", getResponse.Value.StringValue);
    }

    /// <summary>Verifies updating an existing entry above the limit returns ResourceExhausted and preserves the prior value.</summary>
    [Fact]
    public async Task GrpcUpdateAboveLimitReturnsResourceExhaustedAndPreservesValue()
    {
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                CacheName = "default",
                Key = "grpc-update-over-limit",
                Entry = new NodeCacheEntry<object?> { Value = "small", Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.UpdateAsync(
                new UpdateAsyncRequest
                {
                    OperationId = Guid.NewGuid().ToString("N"),
                    CacheName = "default",
                    Key = "grpc-update-over-limit",
                    Entry = new NodeCacheEntry<object?> { Value = value }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Contains(EntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Status.Detail, StringComparison.Ordinal);

        var getResponse = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "grpc-update-over-limit" }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
        Assert.Equal("small", getResponse.Value.StringValue);
    }
}
