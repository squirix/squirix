using System;
using System.Net;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Auth;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies external-access hardening for the primary HTTPS listener.</summary>
public sealed class ExternalAccessHardeningTests : IntegrationTestBase
{
    private const string NodeId = "node-external-hardening";

    /// <summary>Verifies health is served on the primary HTTPS listener.</summary>
    [Fact]
    public async Task HealthEndpointAvailableOnPrimaryHttpsListener()
    {
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(uri, NodeId, security: new TestNodeSecurityOptions());

        var response = await HttpClient.GetAsync(new Uri(uri, "/health"), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>Verifies non-loopback primary listeners start when JWT authentication is configured.</summary>
    [Fact]
    public async Task NonLoopbackListenWithJwtSucceeds()
    {
        var mainPort = AllocateDedicatedPort();
        var uri = new UriBuilder(Uri.UriSchemeHttps, "0.0.0.0", mainPort).Uri;

        await using var node = await StartNodeAsync(uri, NodeId, security: TestJwtHelper.ToSecurityOptions(TestJwtHelper.CreateRandomCredentials()));

        var clientUri = new UriBuilder(Uri.UriSchemeHttps, "127.0.0.1", mainPort).Uri;
        using var channel = CreateGrpcChannel(clientUri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "auth-required" }, cancellationToken: DefaultCancellationToken).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies non-loopback primary listeners require authentication.</summary>
    [Fact]
    public async Task ProductionExternalUrlRequiresAuthentication()
    {
        var mainPort = AllocateDedicatedPort();
        var uri = new UriBuilder(Uri.UriSchemeHttps, "0.0.0.0", mainPort).Uri;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StartNodeAsync(uri, NodeId, security: new TestNodeSecurityOptions()).AsTask());
        Assert.Contains("JWT", ex.Message, StringComparison.Ordinal);
    }
}
