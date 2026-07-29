using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies public HTTP Prometheus scrape redacts identifying labels.</summary>
public sealed class MetricsScrapePrivacyTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-metrics-privacy";

    /// <summary>Verifies authenticated scrape output does not expose raw cache namespace names.</summary>
    [Fact]
    public async Task AuthenticatedMetricsScrapeOmitsCacheNamespaceNames()
    {
        const string secretCacheName = "privacy-integration-cache-7f3a";
        var mainPort = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", mainPort);

        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        var cache = node.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>(secretCacheName);
        await cache.SetEntryAsync(IntegrationMutationOpIds.Default, secretCacheName, "k", new NodeCacheEntry<object?> { Value = "v", Version = 1 }, DefaultCancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", mainPort, "/metrics"));
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync(DefaultCancellationToken);
        Assert.DoesNotContain($"cache=\"{secretCacheName}\"", body, StringComparison.InvariantCulture);
        Assert.DoesNotContain(secretCacheName, body, StringComparison.InvariantCulture);
        Assert.DoesNotContain("exception_type=", body, StringComparison.InvariantCulture);
    }
}
