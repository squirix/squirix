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

/// <summary>Quorum reads default off: reads are served locally without consulting a majority until the switch is on.</summary>
[Immutable]
public sealed class QuorumReadActivationTests : ServerUnitTestBase
{
    /// <summary>With quorum reads disabled, an RF=3 current read without quorum confirmation is rejected.</summary>
    [Test]
    public async Task DisabledFlagRejectsRfThreeCurrentRead()
    {
        var rejected = LeaderAuthorityGate.CheckRead(3, true, true, 7, 7, new LeaderReadState(false, 9, 9));
        _ = await Assert.That(rejected.Allowed).IsFalse();
        _ = await Assert.That(rejected.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);
    }

    /// <summary>Explicit post-proof opt-in serves quorum reads only after verified quorum and applied index.</summary>
    [Test]
    public async Task QuorumReadsEnableAfterProofMatrix()
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
        _ = await Assert.That(proof.QuorumReadsEnabled).IsTrue();

        var gated = FailoverActivationGate.CheckQuorumRead(false, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(gated.Allowed).IsFalse();
        _ = await Assert.That(gated.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);

        var single = FailoverActivationGate.CheckQuorumRead(true, 1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        _ = await Assert.That(single.Allowed).IsTrue();

        var allowed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(allowed.Allowed).IsTrue();

        var unconfirmed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(false, 9, 9));
        _ = await Assert.That(unconfirmed.Allowed).IsFalse();
        _ = await Assert.That(unconfirmed.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);

        var lagging = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 4, 5));
        _ = await Assert.That(lagging.Allowed).IsFalse();
        _ = await Assert.That(lagging.Denial).IsEqualTo(LeaderAuthorityDenial.ReadIndexNotApplied);

        var minority = FailoverActivationGate.CheckQuorumRead(true, 3, false, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(minority.Allowed).IsFalse();
        _ = await Assert.That(minority.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var deposed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 7, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(deposed.Allowed).IsFalse();
        _ = await Assert.That(deposed.Denial).IsEqualTo(LeaderAuthorityDenial.StaleTerm);
    }
}
