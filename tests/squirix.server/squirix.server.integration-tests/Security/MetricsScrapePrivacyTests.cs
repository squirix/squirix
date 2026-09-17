using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies public HTTP Prometheus scrape redacts identifying labels.</summary>
public sealed class MetricsScrapePrivacyTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-metrics-privacy";

    /// <summary>Verifies authenticated scrape output does not expose raw cache namespace names.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthenticatedScrapeOmitsCacheNames(CancellationToken cancellationToken)
    {
        const string secretCacheName = "privacy-integration-cache-7f3a";
        using var held = AllocateDedicatedPort();
        var uri = NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", held.Port);

        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        var cache = node.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>(secretCacheName);
        await cache.SetEntryAsync(IntegrationMutationOpIds.Default, secretCacheName, "k", new NodeCacheEntry<object?> { Value = "v", Version = 1 }, cancellationToken);

        using var req = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", held.Port, "/metrics"));
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        _ = await Assert.That(body).DoesNotContain($"cache=\"{secretCacheName}\"", StringComparison.InvariantCulture);
        _ = await Assert.That(body).DoesNotContain(secretCacheName, StringComparison.InvariantCulture);
        _ = await Assert.That(body).DoesNotContain("exception_type=", StringComparison.InvariantCulture);
    }
}
