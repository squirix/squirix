using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Activated topology agreement across restarts: matching identity stays ready, changes are refused.</summary>
public sealed class TopologyAgreementTests : NodeIntegrationTestBase
{
    /// <summary>A restart with a new generation and no offline bootstrap is refused at startup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GenerationChangeRejectedWithoutBootstrap(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-generation" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        var opt = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-generation", ConfigurationGeneration = 2 };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(StartClusterNodeAsync(uriA, cluster.Peers, opt, cancellationToken));

        _ = await Assert.That(exception.Message).Contains("offline bootstrap", StringComparison.Ordinal);
    }

    /// <summary>A restart with the same activated identity starts and its group logs stay ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartMatchingAllowsReadiness(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-restart" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        await using var restarted = await StartClusterNodeAsync(
            uriA,
            cluster.Peers,
            new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-restart" },
            cancellationToken);
        var registry = restarted.GetRequiredService<ReplicaGroupRegistry>();

        _ = await Assert.That(registry.TryGetLog("n1", out var log)).IsTrue();
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }
}
