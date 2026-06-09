using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Http;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies transport hardening behavior for plaintext HTTP sidecar exposure.
/// </summary>
public sealed class TransportExposureHardeningTests : IntegrationTestBase
{
    private static readonly HttpClient SidecarClient = LoopbackHttp.CreateRestClient(TimeSpan.FromSeconds(5));

    /// <summary>
    /// Verifies non-loopback primary listeners start when API key authentication is configured.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task NonLoopbackListenWithApiKeysSucceeds()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions { ApiKeys = ["external-secret"] });

        using var channel = CreateGrpcChannel($"https://127.0.0.1:{mainPort}");
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "auth-required" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies non-loopback primary listeners start only with explicit unauthenticated external override.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task NonLoopbackListenWithExplicitAllowSucceeds()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions(),
            transportExposure: new TestNodeTransportExposureOptions { AllowUnauthenticatedExternal = true });

        using var channel = CreateGrpcChannel($"https://127.0.0.1:{mainPort}");
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var response = await client.ContainsAsync(new ContainsRequest { Key = "open-external", CacheName = "default" }, cancellationToken: DefaultCancellationToken);
        Assert.False(response.Exists);
    }

    /// <summary>
    /// Verifies plaintext sidecar listener is allowed for loopback binding.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task PlaintextHttpSidecarOnLoopbackIsAllowed()
    {
        var url = GetNextHttpUrl();
        var sidecarPort = AllocateDedicatedPort();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions(),
            transportExposure: new TestNodeTransportExposureOptions { Http1SidecarPort = sidecarPort });

        var response = await SidecarClient.GetAsync($"http://127.0.0.1:{sidecarPort}/health", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies non-loopback plaintext sidecar is rejected without explicit insecure override.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task PlaintextHttpSidecarOnNonLoopbackWithoutOverrideIsRejected()
    {
        var mainPort = AllocateDedicatedPort();
        var sidecarPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions(),
            transportExposure: new TestNodeTransportExposureOptions
            {
                Http1SidecarPort = sidecarPort,
                AllowUnauthenticatedExternal = true,
            }));
        Assert.Contains("SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies non-loopback plaintext sidecar is allowed only with explicit insecure override.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task PlaintextHttpSidecarOnNonLoopbackWithOverrideIsAllowed()
    {
        var mainPort = AllocateDedicatedPort();
        var sidecarPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions(),
            transportExposure: new TestNodeTransportExposureOptions
            {
                Http1SidecarPort = sidecarPort,
                AllowUnauthenticatedExternal = true,
                AllowInsecureHttp1SidecarExternal = true,
            });

        var response = await SidecarClient.GetAsync($"http://127.0.0.1:{sidecarPort}/health", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies non-loopback primary listeners require authentication or an explicit insecure override.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ProductionExternalUrlRequiresAuthenticationOrExplicitAllow()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions()));
        Assert.Contains("SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQUIRIX_API_KEYS", ex.Message, StringComparison.Ordinal);
    }
}
