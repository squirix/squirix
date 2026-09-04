using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies external JWT auth and internal cluster mTLS auth remain separated.</summary>
public sealed class InternalClusterAuthIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>Verifies an external caller cannot spoof internal owner-routing metadata without trusted cluster mTLS.</summary>
    [Fact]
    public async Task ExternalClientCannotSpoofOwnerHeader()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(uri, "node-a", new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata
        {
            { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" },
            { "squirix-internal-owner-rpc", "true" },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "spoofed-internal-marker" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies cluster forwarding over trusted inter-node mTLS succeeds without propagating external JWT.</summary>
    [Fact]
    public async Task ForwardingAcceptsJwtOnInternalTransport()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers);
        await using var nodeB = await StartNodeAsync(uriB, peers);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward");
        const string value = "cluster-forwarded-value";

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var setResponse = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        Assert.True(setResponse.Added);

        using var channelB = CreateGrpcChannel(uriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);

        Assert.True(getResponse.Found);
    }

    /// <summary>Verifies the internal mTLS listener rejects callers that do not present a trusted peer certificate.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer inter-node URL is missing.</exception>
    [Fact]
    public async Task InternalListenerNeedsTrustedPeerCert()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers);
        await using var nodeB = await StartNodeAsync(uriB, peers);

        var interNodeUrl = ThrowHelper.Required(FindPeer(peers, "node-b").InterNodeUri, "Expected inter-node URL for node-b.");

        using var channel = GrpcChannel.ForAddress(
            interNodeUrl,
            new GrpcChannelOptions
            {
                HttpHandler = await CreateCaTrustingHandlerAsync("node-b", peers, DefaultCancellationToken),
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "squirix-internal-owner-rpc", "true" } };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "internal-no-cert" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.True(ex.StatusCode is StatusCode.Unauthenticated or StatusCode.Unavailable or StatusCode.Internal or StatusCode.Unknown);
    }

    /// <summary>Verifies external JWT auth on the primary listener does not need to propagate to inter-node forwarding.</summary>
    [Fact]
    public async Task JwtAuthForwardingUsesInternalMtls()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-forward");
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward-jwt");
        const string value = "cluster-forwarded-with-jwt";

        using var channelA = CreateGrpcChannel(uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var setResponse = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        Assert.True(setResponse.Added);
    }

    /// <summary>Verifies trusted inter-node mTLS with internal owner-routing metadata is rejected when the key is not owned locally.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer inter-node URL is missing.</exception>
    [Fact]
    public async Task OwnerRpcWrongNodeReturnsStaleOwner()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers);
        await using var nodeB = await StartNodeAsync(uriB, peers);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "stale-owner-routing");
        var nodeBUrl = FindPeer(peers, "node-b").Uri;
        var interNodeUrlA = ThrowHelper.Required(FindPeer(peers, "node-a").InterNodeUri, "Expected inter-node URL for node-a.");

        using var channel = GrpcChannel.ForAddress(
            interNodeUrlA,
            new GrpcChannelOptions
            {
                HttpHandler = await CreateTrustedInterNodeClientHandlerAsync("node-b", nodeBUrl, "node-a", peers, DefaultCancellationToken),
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "squirix-internal-owner-rpc", "true" } };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = "stale-owner-blocked", Version = 1 }.MapToProto(),
                },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("owned by 'node-b'", ex.Status.Detail, StringComparison.Ordinal);
        Assert.Equal("stale-owner", ex.Trailers.GetValue("squirix-error-code"));
    }

    /// <summary>Verifies internal owner-routing metadata is rejected on the external listener even with JWT auth.</summary>
    [Fact]
    public async Task SpoofedInternalOwnerHeaderRejected()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var channel = CreateGrpcChannel(uriB);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata
        {
            { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" },
            { "squirix-internal-owner-rpc", "true" },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "spoofed-owner-write",
                    Entry = new NodeCacheEntry<object?> { Value = "blocked", Version = 1 }.MapToProto(),
                },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    private static ServerPeer FindPeer(IReadOnlyList<ServerPeer> peers, string nodeId)
    {
        foreach (var peer in peers)
        {
            if (string.Equals(peer.NodeId, nodeId, StringComparison.OrdinalIgnoreCase))
                return peer;
        }

        throw new InvalidOperationException("Expected peer was not found.");
    }
}
