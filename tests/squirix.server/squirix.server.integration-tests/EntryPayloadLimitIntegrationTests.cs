using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for fixed entry payload size limits (issue #2).</summary>
public sealed class EntryPayloadLimitIntegrationTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-payload-limit";

    /// <summary>Verifies cluster forwarding preserves ResourceExhausted when the remote owner rejects an oversized entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardRejectsTooLargeRemotePayload(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, cancellationToken: cancellationToken);
        await using var nodeB = await StartNodeAsync(uriB, peers, cancellationToken: cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "payload-limit");
        var value = await EntryLimitKit.CreateStringOverEntryLimitAsync();

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            clientA.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);

        using var channelB = CreateGrpcChannel(uriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsFalse();
    }

    /// <summary>Verifies cluster forwarding preserves ResourceExhausted when a remote owner rejects an oversized update.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedUpdatePreservesLargePayload(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, cancellationToken: cancellationToken);
        await using var nodeB = await StartNodeAsync(uriB, peers, cancellationToken: cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "payload-limit-update");

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);

        _ = await clientA.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "small", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        var value = await EntryLimitKit.CreateStringOverEntryLimitAsync();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            clientA.UpdateAsync(
                new UpdateAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = value }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);

        var getResponse = await clientA.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsTrue();
        _ = await Assert.That(getResponse.Value.StringValue).IsEqualTo("small");
    }

    /// <summary>Verifies gRPC insert above the limit returns ResourceExhausted and does not persist.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedInsertReturnsResourceExhausted(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, cancellationToken: cancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var value = await EntryLimitKit.CreateStringOverEntryLimitAsync();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "grpc-over-limit",
                    Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(ex.Status.Detail).Contains("4194304", StringComparison.Ordinal);

        var getResponse = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-over-limit" }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsFalse();
    }

    /// <summary>Verifies updating an existing entry above the limit returns ResourceExhausted and preserves the prior value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedUpdateKeepsOriginalValue(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, cancellationToken: cancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = "grpc-update-over-limit",
                Entry = new NodeCacheEntry<object?> { Value = "small", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        var value = await EntryLimitKit.CreateStringOverEntryLimitAsync();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.UpdateAsync(
                new UpdateAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "grpc-update-over-limit",
                    Entry = new NodeCacheEntry<object?> { Value = value }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(ex.Status.Detail).Contains("4194304", StringComparison.Ordinal);

        var getResponse = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "grpc-update-over-limit" }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsTrue();
        _ = await Assert.That(getResponse.Value.StringValue).IsEqualTo("small");
    }
}
