using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Xunit;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>End-to-end coverage for inter-node mTLS cluster forwarding and failure modes.</summary>
public sealed class InterNodeMtlsTests : EndToEndTestBase
{
    /// <summary>Verifies a client connected to node A forwards owner mutations to node B over trusted inter-node mTLS.</summary>
    [Fact]
    public async Task ClientOnNodeAForwardsToOwnerNodeBOverMtls()
    {
        await using var cluster = await TwoNodeSupport.StartTwoNodeNamedCachesAsync<object?>(DefaultCancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-mtls-forward");
        await using var client = await LoopbackConnect.ConnectAsync(cluster.NodeAAddress, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cache.SetAsync(key, "forwarded", cancellationToken: DefaultCancellationToken);

        Assert.Equal("forwarded", (await cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies an external client cannot spoof internal owner-routing metadata on the primary listener.</summary>
    [Fact]
    public async Task ExternalClientCannotSpoofInternalOwnerHeader()
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var bearerToken = JwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await HostedCluster.StartTwoNodeAsync(new TwoNodeStartOptions { Security = security }, cancellationToken: DefaultCancellationToken);
        var status = await InterNodeGrpcProbe.TryGetValueAsync(cluster.GetUri("nodeB"), bearerToken, true, DefaultCancellationToken);
        Assert.Equal(StatusCode.Unauthenticated, status);
    }

    /// <summary>Verifies external JWT authentication works independently of inter-node mTLS forwarding.</summary>
    [Fact]
    public async Task ExternalJwtAuthWorksIndependentlyFromInterNodeMtls()
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var bearerToken = JwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await HostedCluster.StartTwoNodeAsync(new TwoNodeStartOptions { Security = security }, cancellationToken: DefaultCancellationToken);
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-jwt-mtls");
        var provider = CreateBearerTokenProvider(bearerToken);
        var nodeA = cluster.GetUri("nodeA");
        var nodeB = cluster.GetUri("nodeB");
        await using var clientA = await LoopbackConnect.ConnectAsync(nodeA, provider, DefaultCancellationToken);
        await using var clientB = await LoopbackConnect.ConnectAsync(nodeB, provider, DefaultCancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", DefaultCancellationToken);
        var cacheB = await clientB.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cacheA.SetAsync(key, "jwt-forwarded", cancellationToken: DefaultCancellationToken);

        Assert.Equal("jwt-forwarded", (await cacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>Verifies node B rejects inter-node forwarding when node A presents a certificate signed by an untrusted CA.</summary>
    [Fact]
    public async Task ForwardFailsCallerUntrustedClientCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = TestNodeProfile.UntrustedOutboundClientCertificate });
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-client");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>Verifies node A rejects inter-node forwarding when node B presents an untrusted server certificate.</summary>
    [Fact]
    public async Task ForwardFailsOwnerUntrustedServerCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeBProfile = TestNodeProfile.UntrustedInboundServerCertificate });
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-server");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>Verifies node B rejects inter-node forwarding when node A does not present a client certificate.</summary>
    [Fact]
    public async Task ForwardFailsWhenCallerPresentsNoClientCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = TestNodeProfile.NoOutboundClientCertificate });
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-no-client-cert");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>Verifies expired peer certificates are rejected for inter-node forwarding.</summary>
    [Fact]
    public async Task ForwardFailsWhenPeerCertificateIsExpired()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = TestNodeProfile.ExpiredPeerCertificate });
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-expired-peer");

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>Verifies a two-node cluster with inter-node mTLS enabled starts and serves SDK traffic.</summary>
    [Fact]
    public async Task TwoNodeClusterWithInterNodeMtlsStartsSuccessfully()
    {
        await using var cluster = await TwoNodeSupport.StartTwoNodeNamedCachesAsync<object?>(DefaultCancellationToken);

        await cluster.CacheA.SetAsync("mtls-startup", "ok", cancellationToken: DefaultCancellationToken);

        Assert.Equal("ok", (await cluster.CacheB.GetValueAsync("mtls-startup", DefaultCancellationToken)).Value);
    }

    private static void AssertForwardRejected(RpcException exception) =>
        Assert.True(exception.StatusCode is StatusCode.Unavailable or StatusCode.Internal or StatusCode.Unknown or StatusCode.DeadlineExceeded);

    private static Func<CancellationToken, ValueTask<string>> CreateBearerTokenProvider(string token) => new FixedBearerTokenProvider(token).ProvideAsync;

    private static async Task<TwoNodeNamedCaches<object?>> StartTwoNodeCachesWithProfilesAsync(TwoNodeStartOptions startOptions, [CallerMemberName] string testName = "")
    {
        var cluster = await HostedCluster.StartTwoNodeAsync(startOptions, testName, cancellationToken: DefaultCancellationToken);
        try
        {
            var clientA = await cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
            var clientB = await cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
            return await TwoNodeNamedCaches<object?>.CreateAsync(cluster, clientA, clientB, DefaultCancellationToken);
        }
        catch (RpcException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (IOException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (InvalidOperationException)
        {
            await cluster.DisposeAsync();
            throw;
        }
    }
}
