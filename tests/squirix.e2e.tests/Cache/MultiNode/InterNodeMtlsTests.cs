using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>End-to-end coverage for internode mTLS cluster forwarding and failure modes.</summary>
[Immutable]
public sealed class InterNodeMtlsTests : EndToEndTestBase
{
    /// <summary>Verifies a two-node cluster with internode mTLS enabled starts and serves SDK traffic.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClusterWithInterNodeMtlsStartsUp(CancellationToken cancellationToken)
    {
        await using var cluster = await TwoNodeSupport.StartTwoNodeNamedCachesAsync<object?>(cancellationToken);

        await cluster.CacheA.SetAsync("mtls-startup", "ok", cancellationToken: cancellationToken);

        _ = await Assert.That((await cluster.CacheB.GetValueAsync("mtls-startup", cancellationToken)).Value).IsEqualTo("ok");
    }

    /// <summary>Verifies an external client cannot spoof internal owner-routing metadata on the primary listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalClientCannotSpoofOwnerHeader(CancellationToken cancellationToken)
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var bearerToken = JwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await HostedCluster.StartTwoNodeAsync(new MultiNodeStartOptions { Security = security }, cancellationToken: cancellationToken);
        var status = await InterNodeGrpcProbe.GetValueAsync(cluster.GetUri("nodeB"), bearerToken, true, cancellationToken);
        _ = await Assert.That(status).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies node B rejects internode forwarding when node A presents a certificate signed by an untrusted CA.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFailsUntrustedCallerCert(CancellationToken cancellationToken)
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(
            new MultiNodeStartOptions { NodeAProfile = TestNodeProfile.UntrustedOutboundClientCertificate },
            cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-client");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: cancellationToken));

        await AssertForwardRejectedAsync(ex);
    }

    /// <summary>Verifies node A rejects internode forwarding to node C with an untrusted server certificate while node B traffic still flows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFailsUntrustedNodeCCert(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { NodeCProfile = TestNodeProfile.UntrustedInboundServerCertificate };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(ForwardFailsUntrustedNodeCCert), options, cancellationToken: cancellationToken);
        var rejectedKey = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("orders", "nodeC", "e2e-untrusted-node-c");
        await using var clientA = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cacheA.SetAsync(rejectedKey, "blocked", cancellationToken: cancellationToken));

        await AssertForwardRejectedAsync(ex);

        // Only node C is distrusted: traffic to healthy node B keeps flowing through node A.
        var healthyKey = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("orders", "nodeB", "e2e-trusted-node-b");
        await cacheA.SetAsync(healthyKey, "ok", cancellationToken: cancellationToken);
        _ = await Assert.That((await cacheA.GetValueAsync(healthyKey, cancellationToken)).Value).IsEqualTo("ok");
    }

    /// <summary>Verifies node A rejects internode forwarding when node B presents an untrusted server certificate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFailsUntrustedOwnerCert(CancellationToken cancellationToken)
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(
            new MultiNodeStartOptions { NodeBProfile = TestNodeProfile.UntrustedInboundServerCertificate },
            cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-server");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: cancellationToken));

        await AssertForwardRejectedAsync(ex);
    }

    /// <summary>Verifies expired peer certificates are rejected for internode forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFailsWhenPeerCertificateIsExpired(CancellationToken cancellationToken)
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new MultiNodeStartOptions { NodeAProfile = TestNodeProfile.ExpiredPeerCertificate }, cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-expired-peer");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: cancellationToken));

        await AssertForwardRejectedAsync(ex);
    }

    /// <summary>Verifies node B rejects internode forwarding when node A does not present a client certificate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFailsWithoutCallerCertificate(CancellationToken cancellationToken)
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(
            new MultiNodeStartOptions { NodeAProfile = TestNodeProfile.NoOutboundClientCertificate },
            cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-no-client-cert");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: cancellationToken));

        await AssertForwardRejectedAsync(ex);
    }

    /// <summary>Verifies external JWT authentication works independently of internode mTLS forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JwtAuthIndependentOfInterNodeMtls(CancellationToken cancellationToken)
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var bearerToken = JwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await HostedCluster.StartTwoNodeAsync(new MultiNodeStartOptions { Security = security }, cancellationToken: cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-jwt-mtls");
        var provider = CreateBearerTokenProvider(bearerToken);
        var nodeA = cluster.GetUri("nodeA");
        var nodeB = cluster.GetUri("nodeB");
        await using var clientA = await LoopbackConnect.ConnectAsync(nodeA, provider, cancellationToken);
        await using var clientB = await LoopbackConnect.ConnectAsync(nodeB, provider, cancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", cancellationToken);
        var cacheB = await clientB.GetCacheAsync<object?>("orders", cancellationToken);

        await cacheA.SetAsync(key, "jwt-forwarded", cancellationToken: cancellationToken);

        _ = await Assert.That((await cacheB.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("jwt-forwarded");
    }

    /// <summary>Verifies a client connected to node A forwards owner mutations to node B overtrusted internode mTLS.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NodeAClientForwardsToOwnerOverMtls(CancellationToken cancellationToken)
    {
        await using var cluster = await TwoNodeSupport.StartTwoNodeNamedCachesAsync<object?>(cancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-mtls-forward");
        await using var client = await LoopbackConnect.ConnectAsync(cluster.NodeAAddress, cancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", cancellationToken);

        await cache.SetAsync(key, "forwarded", cancellationToken: cancellationToken);

        _ = await Assert.That((await cluster.CacheB.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("forwarded");
    }

    private static async Task AssertForwardRejectedAsync(RpcException exception)
    {
        _ = await Assert.That(
            exception.StatusCode == StatusCode.Unavailable || exception.StatusCode == StatusCode.Internal || exception.StatusCode == StatusCode.Unknown ||
            exception.StatusCode == StatusCode.DeadlineExceeded).IsTrue();
    }

    private static Func<CancellationToken, ValueTask<string>> CreateBearerTokenProvider(string token) => new FixedBearerTokenProvider(token).ProvideAsync;

    /// <summary>Starts a two-node cluster with custom node profiles for mTLS failure tests.</summary>
    /// <param name="startOptions">The multi-node start options.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="testName">The calling test name.</param>
    /// <returns>The named caches for both nodes.</returns>
    private static async Task<TwoNodeNamedCaches<object?>> StartTwoNodeCachesWithProfilesAsync(
        MultiNodeStartOptions startOptions,
        CancellationToken cancellationToken,
        [CallerMemberName] string testName = "")
    {
        var cluster = await HostedCluster.StartTwoNodeAsync(startOptions, testName, cancellationToken: cancellationToken);
        try
        {
            var clientA = await cluster.ConnectClientAsync("nodeA", cancellationToken);
            var clientB = await cluster.ConnectClientAsync("nodeB", cancellationToken);
            return await TwoNodeNamedCaches<object?>.CreateAsync(cluster, clientA, clientB, cancellationToken);
        }
        catch (Exception ex) when (ex is RpcException or IOException or InvalidOperationException)
        {
            await cluster.DisposeAsync();
            throw;
        }
    }
}
