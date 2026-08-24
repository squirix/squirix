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

/// <summary>Verifies Prometheus metrics access rules for loopback and remote clients.</summary>
public sealed class MetricsEndpointAccessTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-metrics-access";
    private static readonly SocketsHttpHandler NonLoopbackIpHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient NonLoopbackIpHttpClient = new(NonLoopbackIpHandler, false);

    /// <summary>Verifies authenticated scrapes succeed against a non-loopback listener when server auth is enabled.</summary>
    [Fact]
    public async Task MetricsScrapeWithListenerAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var req = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", mainPort, "/metrics"));
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>Verifies loopback scrapes succeed without credentials when server auth is enabled.</summary>
    [Fact]
    public async Task LoopbackMetricsScrapeWithAuth()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var response = await HttpClient.GetAsync(new Uri(NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", mainPort, "/metrics")), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>Verifies remote scrapes without credentials are rejected when server auth is enabled.</summary>
    [Fact]
    public async Task RemoteMetricsScrapeWithAuth()
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp));

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", mainPort);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var response = await GetMetricsViaLocalIpAsync(localIp, mainPort, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static Task<HttpResponseMessage> GetMetricsViaLocalIpAsync(string localIp, int port, CancellationToken cancellationToken) => NonLoopbackIpHttpClient.GetAsync(
        new Uri(NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp, port, "/metrics")),
        cancellationToken);
}
