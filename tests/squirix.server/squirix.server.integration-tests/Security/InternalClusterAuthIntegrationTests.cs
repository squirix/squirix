using System;
using System.Collections.Generic;
using System.Threading;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies external JWT auth and internal cluster mTLS auth remain separated.</summary>
public sealed class InternalClusterAuthIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>Verifies an external caller cannot spoof internal owner-routing metadata without trusted cluster mTLS.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalClientCannotSpoofOwnerHeader(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");

        await using var cluster = await StartClusterAsync("node-a", new IntegrationStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata
        {
            { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" },
            { "squirix-internal-owner-rpc", "true" },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "spoofed-internal-marker" },
                new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies cluster forwarding over trusted internode mTLS succeeds without propagating external JWT.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardingAcceptsJwtOnInternalTransport(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward");
        const string value = "cluster-forwarded-value";

        using var channelA = CreateGrpcChannel(cluster["node-a"].Uri);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var setResponse = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        _ = await Assert.That(setResponse.Added).IsTrue();

        using var channelB = CreateGrpcChannel(cluster["node-b"].Uri);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);

        _ = await Assert.That(getResponse.Found).IsTrue();
    }

    /// <summary>Verifies the internal mTLS listener rejects callers that do not present a trusted peer certificate.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer internode URL is missing.</exception>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InternalListenerNeedsTrustedPeerCert(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);
        var peers = cluster.Peers;

        var interNodeUrl = ThrowHelper.Required(FindPeer(peers, "node-b").InterNodeUri, "Expected internode URL for node-b.");

        var channelOptions = new GrpcChannelOptions
        {
            HttpHandler = await CreateCaTrustingHandlerAsync("node-b", peers, cancellationToken),
            MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
            MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
        };
        using var channel = GrpcChannel.ForAddress(interNodeUrl, channelOptions);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "squirix-internal-owner-rpc", "true" } };

        var options = new CallOptions(headers, cancellationToken: cancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "internal-no-cert" }, options).ResponseAsync);

        _ = await Assert.That(
                             ex.StatusCode == StatusCode.Unauthenticated || ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.Internal ||
                             ex.StatusCode == StatusCode.Unknown)
                        .IsTrue();
    }

    /// <summary>Verifies external JWT auth on the primary listener does not need to propagate to internode forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JwtAuthForwardingUsesInternalMtls(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-forward");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward-jwt");
        using var channelA = CreateGrpcChannel(cluster["node-a"].Uri);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var setResponse = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "cluster-forwarded-with-jwt", Version = 1 }.MapToProto(),
            },
            new CallOptions(headers, cancellationToken: cancellationToken));

        _ = await Assert.That(setResponse.Added).IsTrue();
    }

    /// <summary>Verifies trusted internode mTLS with internal owner-routing metadata is rejected when the key is not owned locally.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer internode URL is missing.</exception>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerRpcWrongNodeReturnsStaleOwner(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);
        var peers = cluster.Peers;

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "stale-owner-routing");
        var nodeBUrl = FindPeer(peers, "node-b").Uri;
        var interNodeUrlA = ThrowHelper.Required(FindPeer(peers, "node-a").InterNodeUri, "Expected internode URL for node-a.");

        using var channel = GrpcChannel.ForAddress(
            interNodeUrlA,
            new GrpcChannelOptions
            {
                HttpHandler = await CreateTrustedInterNodeClientHandlerAsync("node-b", nodeBUrl, "node-a", peers, cancellationToken),
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
                new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(ex.Status.Detail).Contains("owned by 'node-b'", StringComparison.Ordinal);
        _ = await Assert.That(ex.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
    }

    /// <summary>Verifies internal owner-routing metadata is rejected on the external listener even with JWT auth.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpoofedInternalOwnerHeaderRejected(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);

        using var channel = CreateGrpcChannel(cluster["node-b"].Uri);
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
                new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
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
