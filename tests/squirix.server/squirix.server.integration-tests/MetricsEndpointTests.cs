using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
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
        await using var cluster = CreateCluster([new ClusterNode("node-metrics-a", GetNextHttpUri()), new ClusterNode("node-metrics-b", GetNextHttpUri())]);
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "replication-gauges" };
        var owner = await cluster.StartNodeAsync("node-metrics-a", options, cancellationToken);
        var uri = owner.Uri;

        // A new group takes no part until all of its members were up once: the second node completes the topology and seeds it.
        _ = await cluster.StartNodeAsync("node-metrics-b", options, cancellationToken);

        // The probe feeds the gauges whatever it answers; its status is not checked, because the copy of the second node's group on this node
        // is seeded by that node in the background and may still be unseeded here.
        using var ready = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);

        using var response = await HttpClient.GetAsync(new Uri(uri, "/metrics"), cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        _ = await Assert.That(body).Contains("squirix_replication_term{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_commit_index{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_topology_match{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_status_reports_total{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_log_bytes{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_log_retained_entries{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("squirix_replication_snapshot_index{", StringComparison.Ordinal);
        _ = await Assert.That(body).Contains("scope=\"replication\"", StringComparison.Ordinal);
    }
}
