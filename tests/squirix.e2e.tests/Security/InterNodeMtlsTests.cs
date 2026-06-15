using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Cache.MultiNode;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Auth;
using Squirix.E2ETests.Support.Client;
using Squirix.E2ETests.Support.Cluster;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Xunit;

namespace Squirix.E2ETests.Security;

/// <summary>
/// End-to-end coverage for inter-node mTLS cluster forwarding and failure modes.
/// </summary>
public sealed class InterNodeMtlsTests : TestBase
{
    /// <summary>
    /// Verifies a client connected to node A forwards owner mutations to node B over trusted inter-node mTLS.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ClientOnNodeAForwardsToOwnerNodeBOverMtls()
    {
        await using var cluster = await MultiNodeSupport.StartTwoNodeNamedCachesAsync<object?>();
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-mtls-forward");
        await using var client = await LoopbackConnect.ConnectAsync(cluster.NodeAAddress, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cache.SetAsync(key, "forwarded", cancellationToken: DefaultCancellationToken);

        Assert.Equal("forwarded", (await cluster.CacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies an external client cannot spoof internal owner-routing metadata on the primary listener.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
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

        var status = await MtlsInterNodeGrpcProbe.TryGetValueAsync(cluster.GetAddress("nodeB"), bearerToken, true, DefaultCancellationToken);

        Assert.Equal(StatusCode.Unauthenticated, status);
    }

    /// <summary>
    /// Verifies external JWT authentication works independently of inter-node mTLS forwarding.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
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
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-jwt-mtls");
        await using var clientA = await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(cluster.GetAddress("nodeA"));
                options.BearerTokenProvider = _ => new ValueTask<string>(bearerToken);
            },
            DefaultCancellationToken);
        await using var clientB = await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(cluster.GetAddress("nodeB"));
                options.BearerTokenProvider = _ => new ValueTask<string>(bearerToken);
            },
            DefaultCancellationToken);
        var cacheA = await clientA.GetCacheAsync<object?>("orders", DefaultCancellationToken);
        var cacheB = await clientB.GetCacheAsync<object?>("orders", DefaultCancellationToken);

        await cacheA.SetAsync(key, "jwt-forwarded", cancellationToken: DefaultCancellationToken);

        Assert.Equal("jwt-forwarded", (await cacheB.GetValueAsync(key, DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies node B rejects inter-node forwarding when node A does not present a client certificate.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ForwardFailsWhenCallerPresentsNoClientCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = MtlsTestNodeProfile.NoOutboundClientCertificate });
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-no-client-cert");

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>
    /// Verifies node B rejects inter-node forwarding when node A presents a certificate signed by an untrusted CA.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ForwardFailsWhenCallerPresentsUntrustedClientCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = MtlsTestNodeProfile.UntrustedOutboundClientCertificate });
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-client");

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>
    /// Verifies node A rejects inter-node forwarding when node B presents an untrusted server certificate.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ForwardFailsWhenOwnerPresentsUntrustedServerCertificate()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeBProfile = MtlsTestNodeProfile.UntrustedInboundServerCertificate });
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-untrusted-server");

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>
    /// Verifies expired peer certificates are rejected for inter-node forwarding.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ForwardFailsWhenPeerCertificateIsExpired()
    {
        await using var cluster = await StartTwoNodeCachesWithProfilesAsync(new TwoNodeStartOptions { NodeAProfile = MtlsTestNodeProfile.ExpiredPeerCertificate });
        var key = MultiNodeSupport.FindKeyOwnedBy("orders", "nodeB", "e2e-expired-peer");

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cluster.CacheA.SetAsync(key, "blocked", cancellationToken: DefaultCancellationToken));

        AssertForwardRejected(ex);
    }

    /// <summary>
    /// Verifies a two-node cluster with inter-node mTLS enabled starts and serves SDK traffic.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task TwoNodeClusterWithInterNodeMtlsStartsSuccessfully()
    {
        await using var cluster = await MultiNodeSupport.StartTwoNodeNamedCachesAsync<object?>();

        await cluster.CacheA.SetAsync("mtls-startup", "ok", cancellationToken: DefaultCancellationToken);

        Assert.Equal("ok", (await cluster.CacheB.GetValueAsync("mtls-startup", DefaultCancellationToken)).Value);
    }

    private static void AssertForwardRejected(RpcException exception)
    {
        Assert.True(
            exception.StatusCode is StatusCode.Unavailable or StatusCode.Internal or StatusCode.Unknown or StatusCode.DeadlineExceeded,
            $"Expected transport-level forward failure, got {exception.StatusCode}.");
    }

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
