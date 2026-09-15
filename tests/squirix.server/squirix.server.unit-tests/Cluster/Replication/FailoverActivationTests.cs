using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Automatic failover stays disabled until failover activation.</summary>
[Immutable]
public sealed class FailoverActivationTests : ServerUnitTestBase
{
    /// <summary>Explicit post-proof opt-in enables RF=3 election; every other shape stays fenced.</summary>
    [Test]
    public async Task AutomaticFailoverEnablesAfterProofMatrix()
    {
        var uri = new Uri("https://localhost:6001");
        var proof = new TopologyOptions(
        [
            new ServerPeer { NodeId = "node-a", Uri = uri },
            new ServerPeer { NodeId = "node-b", Uri = uri },
            new ServerPeer { NodeId = "node-c", Uri = uri },
        ])
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
            ReplicaCount = 3,
            AutomaticFailoverEnabled = true,
            QuorumReadsEnabled = true,
        };
        _ = await Assert.That(proof.AutomaticFailoverEnabled).IsTrue();
        _ = await Assert.That(proof.QuorumReadsEnabled).IsTrue();

        var eligible = FailoverActivationGate.CheckElection(3, true, true, true, 4, 4);
        _ = await Assert.That(eligible.Eligible).IsTrue();
        _ = await Assert.That(eligible.Denial).IsEqualTo(FailoverDenial.None);

        var disabled = FailoverActivationGate.CheckElection(3, false, true, true, 4, 4);
        _ = await Assert.That(disabled.Eligible).IsFalse();
        _ = await Assert.That(disabled.Denial).IsEqualTo(FailoverDenial.Disabled);

        var single = FailoverActivationGate.CheckElection(1, true, true, true, 1, 1);
        _ = await Assert.That(single.Eligible).IsFalse();
        _ = await Assert.That(single.Denial).IsEqualTo(FailoverDenial.SingleNode);

        var pair = FailoverActivationGate.CheckElection(2, true, true, true, 1, 1);
        _ = await Assert.That(pair.Eligible).IsFalse();
        _ = await Assert.That(pair.Denial).IsEqualTo(FailoverDenial.ReplicaFactorTooLow);

        var minority = FailoverActivationGate.CheckElection(3, true, false, true, 4, 4);
        _ = await Assert.That(minority.Eligible).IsFalse();
        _ = await Assert.That(minority.Denial).IsEqualTo(FailoverDenial.NoMajority);

        var behind = FailoverActivationGate.CheckElection(3, true, true, false, 4, 4);
        _ = await Assert.That(behind.Eligible).IsFalse();
        _ = await Assert.That(behind.Denial).IsEqualTo(FailoverDenial.LogNotCaughtUp);

        var deposed = FailoverActivationGate.CheckElection(3, true, true, true, 4, 5);
        _ = await Assert.That(deposed.Eligible).IsFalse();
        _ = await Assert.That(deposed.Denial).IsEqualTo(FailoverDenial.StaleTerm);
    }

    /// <summary>The internal failover switch defaults off for any replica factor and no feature state enables it.</summary>
    [Test]
    public async Task AutomaticFailoverRemainsDisabled()
    {
        var uri = new Uri("https://localhost:6001");
        var single = new TopologyOptions(new ServerPeer { NodeId = "node-a", Uri = uri })
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
        };
        _ = await Assert.That(single.AutomaticFailoverEnabled).IsFalse();

        var triple = new TopologyOptions(
        [
            new ServerPeer { NodeId = "node-a", Uri = uri },
            new ServerPeer { NodeId = "node-b", Uri = uri },
            new ServerPeer { NodeId = "node-c", Uri = uri },
        ])
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
            ReplicaCount = 3,
        };
        _ = await Assert.That(triple.AutomaticFailoverEnabled).IsFalse();

        _ = await Assert.That(FeatureState.Disabled).IsEqualTo(new FeatureState(false, false));
        _ = await Assert.That(FeatureState.Foundation).IsEqualTo(new FeatureState(false, true));
        _ = await Assert.That(FeatureState.Activated).IsEqualTo(new FeatureState(true, false));
    }
}
