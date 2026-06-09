using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies Prometheus metrics access rules for loopback and remote clients.
/// </summary>
public sealed class MetricsEndpointAccessTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies loopback scrapes succeed without credentials when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task LoopbackMetricsScrapeSucceedsWithoutCredentialsWhenAuthEnabled()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["metrics-secret"] });

        var response = await HttpClient.GetAsync($"https://127.0.0.1:{mainPort}/metrics", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
