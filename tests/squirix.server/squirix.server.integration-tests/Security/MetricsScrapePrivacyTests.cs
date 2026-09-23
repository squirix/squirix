using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
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
        var uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", held.Port), UriKind.Absolute);

        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var cluster = await StartClusterAsync(
            new ClusterNode(NodeId, uri),
            new IntegrationStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);

        var cache = cluster[NodeId].GetCache<object?>(secretCacheName);
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
