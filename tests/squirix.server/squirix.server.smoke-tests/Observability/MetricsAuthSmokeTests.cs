using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.SmokeTests.Observability;

/// <summary>
/// Smoke tests verifying JWT auth rules on the Prometheus-compatible <c>/metrics</c> endpoint.
/// </summary>
/// <remarks>
/// Non-loopback scrape rejection is covered in integration <c>MetricsEndpointAccessTests</c>.
/// </remarks>
public sealed class MetricsAuthSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Verifies loopback <c>/metrics</c> scrapes stay anonymous when JWT auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task LoopbackMetricsScrapeSucceedsWithoutJwtWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-metrics-auth", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var loopback = await HttpClient.GetAsync($"{url}/metrics", DefaultCancellationToken);
        Assert.True(loopback.IsSuccessStatusCode, $"Expected loopback scrape success, got {(int)loopback.StatusCode} {loopback.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies authenticated <c>/metrics</c> scrapes succeed with a bearer token.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task MetricsScrapeSucceedsWithJwtWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-metrics-auth", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        using var request = new HttpRequestMessage(HttpMethod.Get, $"{url}/metrics");
        request.Version = HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var authenticated = await HttpClient.SendAsync(request, DefaultCancellationToken);
        Assert.True(authenticated.IsSuccessStatusCode, $"Expected success with JWT, got {(int)authenticated.StatusCode} {authenticated.ReasonPhrase}");
    }
}
