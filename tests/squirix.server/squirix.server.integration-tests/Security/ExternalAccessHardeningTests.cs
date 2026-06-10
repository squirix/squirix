using System;
using System.Net;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Security;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies external-access hardening for the primary HTTPS listener.
/// </summary>
public sealed class ExternalAccessHardeningTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies non-loopback primary listeners start when JWT authentication is configured.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task NonLoopbackListenWithJwtSucceeds()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(TestJwtHelper.CreateRandomCredentials()));

        using var channel = CreateGrpcChannel($"https://127.0.0.1:{mainPort}");
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "auth-required" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies health is served on the primary HTTPS listener.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task HealthEndpointAvailableOnPrimaryHttpsListener()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions());

        var response = await HttpClient.GetAsync($"{url}/health", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies non-loopback primary listeners require authentication.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ProductionExternalUrlRequiresAuthentication()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions()));
        Assert.Contains("JWT", ex.Message, StringComparison.Ordinal);
    }
}
