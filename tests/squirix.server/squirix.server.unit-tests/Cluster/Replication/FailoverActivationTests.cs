using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Automatic failover stays disabled until failover activation.</summary>
[Immutable]
public sealed class FailoverActivationTests : ServerUnitTestBase
{
    /// <summary>The internal failover switch defaults off for any replica factor and no feature state enables it.</summary>
    [Fact]
    public void AutomaticFailoverRemainsDisabled()
    {
        var uri = new Uri("https://localhost:6001");
        var single = new TopologyOptions(new ServerPeer { NodeId = "node-a", Uri = uri })
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
        };
        Assert.False(single.AutomaticFailoverEnabled);

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
        Assert.False(triple.AutomaticFailoverEnabled);

        Assert.Equal(new FeatureState(false, false), FeatureState.Disabled);
        Assert.Equal(new FeatureState(false, true), FeatureState.Foundation);
        Assert.Equal(new FeatureState(true, false), FeatureState.Activated);
    }

    /// <summary>Explicit post-proof opt-in enables RF=3 election; every other shape stays fenced.</summary>
    [Fact]
    public void AutomaticFailoverEnablesAfterProofMatrix()
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
        Assert.True(proof.AutomaticFailoverEnabled);
        Assert.True(proof.QuorumReadsEnabled);

        var eligible = FailoverActivationGate.CheckElection(3, true, true, true, 4, 4);
        Assert.True(eligible.Eligible);
        Assert.Equal(FailoverDenial.None, eligible.Denial);

        var disabled = FailoverActivationGate.CheckElection(3, false, true, true, 4, 4);
        Assert.False(disabled.Eligible);
        Assert.Equal(FailoverDenial.Disabled, disabled.Denial);

        var single = FailoverActivationGate.CheckElection(1, true, true, true, 1, 1);
        Assert.False(single.Eligible);
        Assert.Equal(FailoverDenial.SingleNode, single.Denial);

        var pair = FailoverActivationGate.CheckElection(2, true, true, true, 1, 1);
        Assert.False(pair.Eligible);
        Assert.Equal(FailoverDenial.ReplicaFactorTooLow, pair.Denial);

        var minority = FailoverActivationGate.CheckElection(3, true, false, true, 4, 4);
        Assert.False(minority.Eligible);
        Assert.Equal(FailoverDenial.NoMajority, minority.Denial);

        var behind = FailoverActivationGate.CheckElection(3, true, true, false, 4, 4);
        Assert.False(behind.Eligible);
        Assert.Equal(FailoverDenial.LogNotCaughtUp, behind.Denial);

        var deposed = FailoverActivationGate.CheckElection(3, true, true, true, 4, 5);
        Assert.False(deposed.Eligible);
        Assert.Equal(FailoverDenial.StaleTerm, deposed.Denial);
    }
}
