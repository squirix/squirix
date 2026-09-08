using System;
using System.Net;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies replication series are exposed on the Prometheus metrics endpoint.</summary>
public sealed class MetricsEndpointTests : NodeIntegrationTestBase
{
    /// <summary>Verifies a readiness probe feeds replication gauges visible on the metrics endpoint.</summary>
    [Fact]
    public async Task MetricsEndpointServesReplicationGauges()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-metrics-a", uriA), ("node-metrics-b", uriB)]);
        await using var node = await StartNodeAsync(uriA, peers, new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "replication-gauges" });

        using (var ready = await HttpClient.GetAsync(new Uri(node.Uri, "/health/ready"), DefaultCancellationToken))
            Assert.Equal(HttpStatusCode.OK, ready.StatusCode);

        using var response = await HttpClient.GetAsync(new Uri(node.Uri, "/metrics"), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync(DefaultCancellationToken);
        Assert.Contains("squirix_replication_term{", body, StringComparison.Ordinal);
        Assert.Contains("squirix_replication_commit_index{", body, StringComparison.Ordinal);
        Assert.Contains("squirix_replication_topology_match{", body, StringComparison.Ordinal);
        Assert.Contains("squirix_replication_status_reports_total{", body, StringComparison.Ordinal);
        Assert.Contains("scope=\"replication\"", body, StringComparison.Ordinal);
    }
}
