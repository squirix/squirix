using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies Prometheus metrics access rules for loopback and remote clients.</summary>
public sealed class MetricsEndpointAccessTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-metrics-access";
    private static readonly SocketsHttpHandler NonLoopbackIpHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient NonLoopbackIpHttpClient = new(NonLoopbackIpHandler, false);

    /// <summary>Verifies loopback scrapes succeed without credentials when server auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoopbackMetricsScrapeWithAuth(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        using var held = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", held.Port);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        var response = await HttpClient.GetAsync(new Uri(NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", held.Port, "/metrics")), cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies authenticated scrapes succeed against a non-loopback listener when server auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsScrapeWithListenerAuthEnabled(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        using var held = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", held.Port);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", held.Port, "/metrics"));
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies remote scrapes without credentials are rejected when server auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteMetricsScrapeWithAuth(CancellationToken cancellationToken)
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();

        var credentials = TestJwtHelper.CreateRandomCredentials();
        using var held = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", held.Port);

        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        var response = await GetMetricsViaLocalIpAsync(localIp!, held.Port, cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
    }

    private static Task<HttpResponseMessage> GetMetricsViaLocalIpAsync(string localIp, int port, CancellationToken cancellationToken) => NonLoopbackIpHttpClient.GetAsync(
        new Uri(NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp, port, "/metrics")),
        cancellationToken);
}
