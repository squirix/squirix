using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
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
        using var identity = new ClusterIdentity();
        using var dir = new TempDirectory("squirix-e2e-topology-live");
        var peers = new[] { ("nodeA", heldA.HttpUri), ("nodeB", heldB.HttpUri) };
        var dirA = NodePathKit.Combine(dir, "nodeA");
        var dirB = NodePathKit.Combine(dir, "nodeB");
        var optionsA = new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA };
        var optionsB = new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB };

        var hostA = await TestNodeHostFactory.StartNodeAsync("nodeA", heldA.HttpUri, peers, optionsA, identity, cancellationToken);
        await using var hostB = await TestNodeHostFactory.StartNodeAsync("nodeB", heldB.HttpUri, peers, optionsB, identity, cancellationToken);
        await hostA.DisposeAsync();

        // NodeB stays live while nodeA restarts with a peer set that was never bootstrapped.
        var changedPeers = new[] { ("nodeA", heldA.HttpUri), ("nodeC", heldC.HttpUri) };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync("nodeA", heldA.HttpUri, changedPeers, optionsA, identity, cancellationToken));

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
        using var identity = new ClusterIdentity();
        using var dir = new TempDirectory("squirix-e2e-topology-stopped");
        var peers = new[] { ("nodeA", heldA.HttpUri), ("nodeB", heldB.HttpUri) };
        var dirA = NodePathKit.Combine(dir, "nodeA");
        var dirB = NodePathKit.Combine(dir, "nodeB");

        var hostA = await TestNodeHostFactory.StartNodeAsync("nodeA", heldA.HttpUri, peers, new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA }, identity, cancellationToken);
        var hostB = await TestNodeHostFactory.StartNodeAsync("nodeB", heldB.HttpUri, peers, new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirB }, identity, cancellationToken);
        await hostA.DisposeAsync();
        await hostB.DisposeAsync();

        // The whole cluster is stopped, yet the generation change without a bootstrap is refused.
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestNodeHost>(
            TestNodeHostFactory.StartNodeAsync(
                "nodeA",
                heldA.HttpUri,
                peers,
                new TestNodeHostStartOptions { ReplicaCount = 2, DataDir = dirA, ConfigurationGeneration = 2 },
                identity,
                cancellationToken));

        _ = await Assert.That(exception.Message).Contains("offline bootstrap", StringComparison.Ordinal);
    }
}
