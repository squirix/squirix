using System;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Limits;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.Security;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies external JWT auth and internal cluster mTLS auth remain separated.
/// </summary>
public sealed class InternalClusterAuthIntegrationTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies an external caller cannot spoof internal owner-routing metadata without trusted cluster mTLS.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ExternalClientCannotSpoofInternalOwnerHeader()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata
        {
            { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" },
            { "squirix-internal-owner-rpc", "true" },
        };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(
                new GetValueRequest { CacheName = "default", Key = "spoofed-internal-marker" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies cluster forwarding over trusted inter-node mTLS succeeds without propagating external JWT.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task InterNodeForwardingSucceedsWithoutJwtOnInternalTransport()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = BuildClusterPeers(("node-a", urlA), ("node-b", urlB));

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "cluster-forward");
        const string value = "cluster-forwarded-value";

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var setResponse = await clientA.TrySetAsync(
            new TrySetRequest
            {
                CacheName = "default",
                Key = key,
                Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        Assert.True(setResponse.Added);

        using var channelB = CreateGrpcChannel(urlB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetValueAsync(
            new GetValueRequest { CacheName = "default", Key = key },
            cancellationToken: DefaultCancellationToken);

        Assert.True(getResponse.Found);
        Assert.Equal(value, ProtoEx.CacheValueFromGrpcValue<object?>(getResponse.Value, null, null).Value);
    }

    /// <summary>
    /// Verifies external JWT auth on the primary listener does not need to propagate to inter-node forwarding.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ExternalJwtAuthSucceedsWhileClusterForwardingUsesInternalMtls()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-forward");
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = BuildClusterPeers(("node-a", urlA), ("node-b", urlB));

        await using var nodeA = await StartNodeAsync(urlA, peers, security: TestJwtHelper.ToSecurityOptions(credentials));
        await using var nodeB = await StartNodeAsync(urlB, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        var key = new TestKeyOwnerHelper(["node-a", "node-b"]).FindKeyOwnedBy("default", "node-b", "cluster-forward-jwt");
        const string value = "cluster-forwarded-with-jwt";

        using var channelA = CreateGrpcChannel(urlA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var setResponse = await clientA.TrySetAsync(
            new TrySetRequest
            {
                CacheName = "default",
                Key = key,
                Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        Assert.True(setResponse.Added);
    }

    /// <summary>
    /// Verifies internal owner-routing metadata is rejected on the external listener even with JWT auth.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task MultiNodeExternalClientCannotSpoofInternalOwnerHeader()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = BuildClusterPeers(("node-a", urlA), ("node-b", urlB));

        await using var nodeA = await StartNodeAsync(urlA, peers, security: TestJwtHelper.ToSecurityOptions(credentials));
        await using var nodeB = await StartNodeAsync(urlB, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var channel = CreateGrpcChannel(urlB);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata
        {
            { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" },
            { "squirix-internal-owner-rpc", "true" },
        };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.SetValueAsync(
                new SetValueRequest { CacheName = "default", Key = "spoofed-owner-write", Value = ProtoEx.CacheValueToGrpcValue("blocked") },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies the internal mTLS listener rejects callers that do not present a trusted peer certificate.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task InternalListenerRejectsCallsWithoutTrustedPeerCertificate()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = BuildClusterPeers(("node-a", urlA), ("node-b", urlB));

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var interNodeUrl = peers.First(static peer => peer.NodeId == "node-b").InterNodeUrl
            ?? throw new InvalidOperationException("Expected inter-node URL for node-b.");

        using var channel = GrpcChannel.ForAddress(
            interNodeUrl,
            new GrpcChannelOptions
            {
                HttpHandler = CreateClusterCaTrustingHandlerWithoutClientCertificate(urlB, peers),
                MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "squirix-internal-owner-rpc", "true" } };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(
                new GetValueRequest { CacheName = "default", Key = "internal-no-cert" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.True(
            ex.StatusCode is StatusCode.Unauthenticated or StatusCode.Unavailable,
            $"Expected unauthenticated or unavailable, got {ex.StatusCode}.");
    }
}
