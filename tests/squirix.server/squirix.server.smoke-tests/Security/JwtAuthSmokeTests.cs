using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.SmokeTests.Security;

/// <summary>
/// Smoke tests verifying that JWT bearer tokens protect cache endpoints when configured.
/// </summary>
public sealed class JwtAuthSmokeTests : SmokeTestBase
{
    /// <summary>Ensures REST cache endpoints reject requests without a bearer token when JWT auth is configured.</summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task CacheEndpointsRequireJwtWhenConfigured()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://smoke.squirix.test", "smoke-cache");
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-jwt", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var entry = new CacheEntry<string> { Value = "ok", Version = 1 };

        var unauthorized = await HttpClient.PutAsJsonAsync($"{url}/api/v1/cache/jwt-smoke", entry, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, unauthorized.StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"{url}/api/v1/cache/jwt-smoke");
        request.Content = JsonContent.Create(entry);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        request.Version = HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;

        var authorized = await HttpClient.SendAsync(request, DefaultCancellationToken);
        Assert.True(authorized.IsSuccessStatusCode, $"Expected success with JWT, got {(int)authorized.StatusCode} {authorized.ReasonPhrase}");
    }

    /// <summary>Verifies loopback <c>/metrics</c> scrapes stay anonymous when JWT auth is enabled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task MetricsAllowLoopbackWithoutJwtWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-jwt", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var loopback = await HttpClient.GetAsync($"{url}/metrics", DefaultCancellationToken);
        Assert.True(loopback.IsSuccessStatusCode, $"Expected loopback scrape success, got {(int)loopback.StatusCode} {loopback.ReasonPhrase}");
    }

    /// <summary>Verifies authenticated <c>/metrics</c> scrapes succeed with a bearer token.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task MetricsSucceedWithJwtWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-jwt", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{url}/metrics");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var authenticated = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.True(authenticated.IsSuccessStatusCode, $"Expected success with JWT, got {(int)authenticated.StatusCode} {authenticated.ReasonPhrase}");
    }
}
