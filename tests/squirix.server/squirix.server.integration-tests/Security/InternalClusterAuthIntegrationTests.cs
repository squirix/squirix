using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Serialization.Protobuf;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies external JWT auth and internal cluster mTLS auth remain separated.</summary>
public sealed class InternalClusterAuthIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>Verifies the public listener does not expose peer forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicListenerRejectsPeerReads(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");

        await using var cluster = await StartClusterAsync("node-a", new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) }, cancellationToken);

        using var channelHandler = LoopbackHttp.CreateHandler();
        using var channel = new HttpClient(channelHandler, false) { BaseAddress = cluster["node-a"].Uri };
        var client = channel;

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "spoofed-internal-marker" },
                cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Verifies cluster forwarding over trusted internode mTLS succeeds without propagating external JWT.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardingAcceptsJwtOnInternalTransport(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { TimeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow) };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward");
        const string value = "cluster-forwarded-value";

        using var channelA = CreateBinaryChannel(cluster["node-a"].Uri);
        var clientA = new CacheWireClient(channelA);
        var setResponse = await clientA.AddEntryIfAbsentAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        _ = await Assert.That(setResponse.Added).IsTrue();

        using var channelB = CreateBinaryChannel(cluster["node-b"].Uri);
        var clientB = new CacheWireClient(channelB);
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

        using var handler = await CreateCaTrustingHandlerAsync("node-b", peers, cancellationToken);
        using var channel = new HttpClient(handler, false) { BaseAddress = interNodeUrl };
        var client = channel;

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "internal-no-cert" }, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsNull();
    }

    /// <summary>Verifies a ring fingerprint header on the public listener cannot fence a node when it is not sent by a trusted peer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalRingHeaderDoesNotFence(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", "external-ring-header");
        using var channel = CreateBinaryChannel(cluster["node-a"].Uri);
        var client = new CacheWireClient(channel);

        var response = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        await AssertReadyAsync(cluster["node-a"], cancellationToken);

        var spoofed = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { { "squirix-ring-fingerprint", "garbage" } };
        using var legacyHandler = LoopbackHttp.CreateHandler();
        using var legacy = new HttpClient(legacyHandler, false) { BaseAddress = cluster["node-a"].Uri };
        var legacyClient = legacy;
        var rejected = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(legacyClient.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, spoofed, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejected.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
        await AssertReadyAsync(cluster["node-a"], cancellationToken);
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
            new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cluster-forward-jwt");
        using var channelA = CreateBinaryChannel(cluster["node-a"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var clientA = new CacheWireClient(channelA);
        var setResponse = await clientA.AddEntryIfAbsentAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "cluster-forwarded-with-jwt", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

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

        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", nodeBUrl, "node-a", peers, cancellationToken);
        using var channel = new HttpClient(handler, false) { BaseAddress = interNodeUrlA };
        var client = channel;
        var headers = CreateOwnerHeaders(cluster["node-a"].GetRequiredService<RingFingerprint>().Value);

        var ex = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = "stale-owner-blocked", Version = 1 }.MapToProto(),
                },
                headers,
                cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(ex.Message).Contains("owned by 'node-b'", StringComparison.Ordinal);
        _ = await Assert.That(ex.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
    }

    /// <summary>Verifies a trusted internode owner call without a ring fingerprint is refused before it executes and fences the receiver.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer internode URL is missing.</exception>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerRpcWithoutRingFingerprintRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);

        await AssertOwnerRpcRefusedAsync(cluster, CreateOwnerHeaders(null), "no-ring-fingerprint", cancellationToken);
    }

    /// <summary>Verifies a trusted internode owner call with a different ring fingerprint is refused before it executes and fences the receiver.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer internode URL is missing.</exception>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerRpcWithWrongRingFingerprintRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);

        await AssertOwnerRpcRefusedAsync(cluster, CreateOwnerHeaders(new string('0', 64)), "wrong-ring-fingerprint", cancellationToken);
    }

    /// <summary>Verifies trusted internode reads with internal owner-routing metadata are rejected when the key is not owned locally.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the peer internode URL is missing.</exception>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerRpcReadWrongNodeReturnsStaleOwner(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);
        var peers = cluster.Peers;

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "stale-owner-read");
        var nodeBUrl = FindPeer(peers, "node-b").Uri;
        var interNodeUrlA = ThrowHelper.Required(FindPeer(peers, "node-a").InterNodeUri, "Expected internode URL for node-a.");

        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", nodeBUrl, "node-a", peers, cancellationToken);
        using var channel = new HttpClient(handler, false) { BaseAddress = interNodeUrlA };
        var client = channel;
        var headers = CreateOwnerHeaders(cluster["node-a"].GetRequiredService<RingFingerprint>().Value);

        var value = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, headers, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var entry = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, headers, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var expiration = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = "default", Key = key }, headers, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(value.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(value.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(entry.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(entry.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(expiration.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(expiration.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
    }

    /// <summary>Verifies the public listener rejects peer forwarding even with JWT auth.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublicListenerRejectsPeerWritesWithJwt(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "cluster-auth");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);

        using var channelHandler = LoopbackHttp.CreateHandler();
        using var channel = new HttpClient(channelHandler, false) { BaseAddress = cluster["node-b"].Uri };
        var client = channel;

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "spoofed-owner-write",
                    Entry = new NodeCacheEntry<object?> { Value = "blocked", Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    private static Dictionary<string, string> CreateOwnerHeaders(string? ringFingerprint)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (ringFingerprint != null)
            headers.Add("squirix-ring-fingerprint", ringFingerprint);

        return headers;
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

    private async Task AssertOwnerRpcRefusedAsync(TestCluster<IntegrationStartOptions> cluster, Dictionary<string, string> headers, string keySeed, CancellationToken cancellationToken)
    {
        var peers = cluster.Peers;
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", keySeed);
        var interNodeUrlA = ThrowHelper.Required(FindPeer(peers, "node-a").InterNodeUri, "Expected internode URL for node-a.");

        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", FindPeer(peers, "node-b").Uri, "node-a", peers, cancellationToken);
        using var channel = new HttpClient(handler, false) { BaseAddress = interNodeUrlA };
        var client = channel;

        var refused = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = "must-not-run", Version = 1 }.MapToProto(),
                },
                headers,
                cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(refused.FindTrailer("squirix-error-code")).IsEqualTo("ring-mismatch");

        // The receiver is fenced: even a client call is refused, and the refused write left no trace in the cache or the idempotency store.
        using var clientChannel = CreateBinaryChannel(cluster["node-a"].Uri);
        var clientA = new CacheWireClient(clientChannel);
        var fenced = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(clientA.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(fenced.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(fenced.FindTrailer("squirix-error-code")).IsEqualTo("ring-fenced");

        var receiver = cluster["node-a"];
        var stored = receiver.GetRequiredService<PhysicalCache<object?>>().GetValue(new CacheKey("default", key), cancellationToken);
        _ = await Assert.That(stored.Found).IsFalse();
    }

    private async Task AssertReadyAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(node.Uri, "/health/ready"), cancellationToken);

        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }
}
