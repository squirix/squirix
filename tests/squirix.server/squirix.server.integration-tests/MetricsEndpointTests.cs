using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies replication series are exposed on the Prometheus metrics endpoint.</summary>
public sealed class MetricsEndpointTests : NodeIntegrationTestBase
{
    /// <summary>Verifies a readiness probe feeds replication gauges visible on the metrics endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsEndpointServesReplicationGauges(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([new ClusterNode("node-metrics-a", uriA), new ClusterNode("node-metrics-b", uriB)]);
        await using var node = await StartClusterAsync(
            uriA,
            peers,
            new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "replication-gauges" },
            cancellationToken);

        using (var ready = await HttpClient.GetAsync(new Uri(node.Uri, "/health/ready"), cancellationToken))
            _ = await Assert.That(ready.StatusCode).IsEqualTo(HttpStatusCode.OK);

        using var response = await HttpClient.GetAsync(new Uri(node.Uri, "/metrics"), cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        _ = await Assert.That(body).Contains("squirix_replication_term{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_commit_index{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_topology_match{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_status_reports_total{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("scope=\"replication\"", StringComparison.Ordinal);
    }
}
