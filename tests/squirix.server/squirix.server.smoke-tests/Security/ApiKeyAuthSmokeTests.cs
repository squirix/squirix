using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Xunit;

namespace Squirix.Server.SmokeTests.Security;

/// <summary>
/// Smoke tests verifying that API key authentication is enforced on cache endpoints
/// when API key protection is enabled.
/// </summary>
public sealed class ApiKeyAuthSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Verifies that cache endpoints return <c>401 Unauthorized</c> when no API key
    /// is provided, and succeed when a valid <c>X-Api-Key</c> header is supplied.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task CacheEndpointsReturn401WithoutKeyWhenEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["smoke-key"] },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var respNoKey = await HttpClient.PutAsJsonAsync($"{url}/api/v1/cache/smoke", new CacheEntry<string> { Value = "x", Version = 1L }, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, respNoKey.StatusCode);

        using var req = new HttpRequestMessage(HttpMethod.Put, $"{url}/api/v1/cache/smoke");
        req.Content = JsonContent.Create(new CacheEntry<string> { Value = "x", Version = 1L });
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Add("X-Api-Key", "smoke-key");
        var ok = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.True(ok.IsSuccessStatusCode, $"Expected success with API key, got {(int)ok.StatusCode} {ok.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies that cluster diagnostics endpoints are protected by the same API key policy.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ClusterDiagnosticsReturn401WithoutKeyWhenEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["smoke-key"] },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var ringWithoutKey = await HttpClient.GetAsync($"{url}/admin/ring", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, ringWithoutKey.StatusCode);

        var historyWithoutKey = await HttpClient.GetAsync($"{url}/admin/rebalance/history", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, historyWithoutKey.StatusCode);

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{url}/admin/rebalance/history");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Add("X-Api-Key", "smoke-key");

        var ok = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.True(ok.IsSuccessStatusCode, $"Expected success with API key, got {(int)ok.StatusCode} {ok.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies loopback <c>/metrics</c> scrapes stay anonymous when server auth is enabled.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task MetricsAllowLoopbackWithoutKeyWhenAuthEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["smoke-key"] },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var loopback = await HttpClient.GetAsync($"{url}/metrics", DefaultCancellationToken);
        Assert.True(loopback.IsSuccessStatusCode, $"Expected loopback scrape success, got {(int)loopback.StatusCode} {loopback.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies authenticated <c>/metrics</c> scrapes succeed when server auth is enabled.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task MetricsSucceedWithApiKeyWhenAuthEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["smoke-key"] },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{url}/metrics");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Add("X-Api-Key", "smoke-key");
        var authenticated = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.True(authenticated.IsSuccessStatusCode, $"Expected success with API key, got {(int)authenticated.StatusCode} {authenticated.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies that admin storage diagnostics are protected by the same API key policy.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task StorageDiagnosticsReturns401WithoutKeyWhenEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["smoke-key"] },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var respNoKey = await HttpClient.GetAsync($"{url}/admin/diagnostics/storage", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, respNoKey.StatusCode);

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{url}/admin/diagnostics/storage");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Add("X-Api-Key", "smoke-key");

        var ok = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.True(ok.IsSuccessStatusCode, $"Expected success with API key, got {(int)ok.StatusCode} {ok.ReasonPhrase}");
    }
}
