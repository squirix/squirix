using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies public HTTP Prometheus scrape redacts identifying labels.
/// </summary>
public sealed class MetricsScrapePrivacyTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies authenticated scrape output does not expose raw cache namespace names.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task AuthenticatedMetricsScrapeOmitsCacheNamespaceNames()
    {
        const string secretCacheName = "privacy-integration-cache-7f3a";
        var mainPort = AllocateDedicatedPort();
        var url = $"https://127.0.0.1:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        var cache = node.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>(secretCacheName);
        await cache.SetAsync(secretCacheName, "k", new CacheEntry<object?> { Value = "v", Version = 1 }, DefaultCancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{url}/metrics");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync(DefaultCancellationToken);
        Assert.DoesNotContain($"cache=\"{secretCacheName}\"", body);
        Assert.DoesNotContain(secretCacheName, body);
        Assert.DoesNotContain("exception_type=", body);
    }
}
