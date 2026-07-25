using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies readiness details access rules for loopback and remote clients.</summary>
public sealed class ReadyDetailsEndpointAccessTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-ready-details";
    private static readonly SocketsHttpHandler NonLoopbackIpHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient NonLoopbackIpHttpClient = new(NonLoopbackIpHandler, false);

    /// <summary>Verifies authenticated remote scrapes succeed when server auth is enabled.</summary>
    [Fact]
    public async Task AuthenticatedReadyDetailsScrapeListenerAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = InvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var req = new HttpRequestMessage(HttpMethod.Get, InvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", mainPort, "/health/ready/details"));
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>Verifies loopback scrapes succeed without credentials when server auth is enabled.</summary>
    [Fact]
    public async Task LoopbackReadyDetailsScrapeCredentialsAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = InvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var response = await HttpClient.GetAsync(new Uri(InvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", mainPort, "/health/ready/details")), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>Verifies remote scrapes without credentials are rejected when server auth is enabled.</summary>
    [Fact]
    public async Task RemoteReadyDetailsScrapeCredentialsAuthEnabled()
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp));

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = InvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var response = await GetReadyDetailsViaLocalIpAsync(localIp, mainPort, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static Task<HttpResponseMessage> GetReadyDetailsViaLocalIpAsync(string localIp, int port, CancellationToken cancellationToken) => NonLoopbackIpHttpClient.GetAsync(
        new Uri(InvariantIndexStrings.FormatHttpsAbsolute(localIp, port, "/health/ready/details")),
        cancellationToken);
}
