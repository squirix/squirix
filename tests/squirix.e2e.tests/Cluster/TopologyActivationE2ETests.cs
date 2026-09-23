using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>Activated topology changes are rejected outside the offline bootstrap path.</summary>
public sealed class TopologyActivationE2ETests : EndToEndTestBase
{
    /// <summary>A live cluster refuses a restarted node whose peer set was never bootstrapped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveActivatedRfTopologyChangeIsRejected(CancellationToken cancellationToken)
    {
        using var heldA = ListenPortPool.EndToEndTests.HoldPort();
        using var heldB = ListenPortPool.EndToEndTests.HoldPort();
        using var heldC = ListenPortPool.EndToEndTests.HoldPort();
        using var dir = new TempDirectory("squirix-e2e-topology-live");
        ClusterNode[] clusterTopology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        var optionsA = new ClusterStartOptions { ReplicaCount = 2, DataDir = NodePathKit.Combine(dir, "nodeA") };
        var optionsB = new ClusterStartOptions { ReplicaCount = 2, DataDir = NodePathKit.Combine(dir, "nodeB") };
        await using var cluster = TestCluster<ClusterStartOptions>.Create(clusterTopology);
        _ = await cluster.StartNodeAsync("nodeA", optionsA, cancellationToken);
        _ = await cluster.StartNodeAsync("nodeB", optionsB, cancellationToken);
        await cluster.StopNodeAsync("nodeA");

        // NodeB stays live while nodeA restarts with a peer set that was never bootstrapped.
        ClusterNode[] changedTopology = [new("nodeA", heldA.HttpUri), new("nodeC", heldC.HttpUri)];
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync(new ClusterNode("nodeA", heldA.HttpUri), changedTopology, optionsA, cancellationToken));

        _ = await Assert.That(exception.Message).Contains("offline bootstrap", StringComparison.Ordinal);
    }

    /// <summary>A stopped cluster refuses a restart with a generation that was never bootstrapped.</summary>
    /// <remarks>
    /// #239 mandates the name "StoppedActivatedRfTopologyChangeIsRejected"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedActivatedRfTopologyIsRejected(CancellationToken cancellationToken)
    {
        using var heldA = ListenPortPool.EndToEndTests.HoldPort();
        using var heldB = ListenPortPool.EndToEndTests.HoldPort();
        using var dir = new TempDirectory("squirix-e2e-topology-stopped");
        ClusterNode[] clusterTopology = [new("nodeA", heldA.HttpUri), new("nodeB", heldB.HttpUri)];
        var dirA = NodePathKit.Combine(dir, "nodeA");
        var dirB = NodePathKit.Combine(dir, "nodeB");
        await using var cluster = TestCluster<ClusterStartOptions>.Create(clusterTopology);
        _ = await cluster.StartNodeAsync("nodeA", new ClusterStartOptions { ReplicaCount = 2, DataDir = dirA }, cancellationToken);
        _ = await cluster.StartNodeAsync("nodeB", new ClusterStartOptions { ReplicaCount = 2, DataDir = dirB }, cancellationToken);
        await cluster.StopNodeAsync("nodeA");
        await cluster.StopNodeAsync("nodeB");

        // The whole cluster is stopped, yet the generation change without a bootstrap is refused.
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync("nodeA", new ClusterStartOptions { ReplicaCount = 2, DataDir = dirA, ConfigurationGeneration = 2 }, cancellationToken));

        _ = await Assert.That(exception.Message).Contains("offline bootstrap", StringComparison.Ordinal);
    }
}
