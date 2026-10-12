using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The eligibility slots of a group start fresh at each launch of an elected leader.</summary>
[Immutable]
public sealed class ReplicaSlotResetTests
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A leader whose tail is shorter than at an earlier launch is marked ready.</summary>
    [Test]
    public async Task LeaderWithShorterTailIsMarkedReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var earlier = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 9, 1, 9, 0, FollowerLogReadiness.Ready, GroupMemberState.Complete, false);
        var shorter = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 3, 1, 3, 0, FollowerLogReadiness.Ready, GroupMemberState.Complete, false);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in earlier, Fingerprint, 1);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in shorter, Fingerprint, 1);

        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

    /// <summary>A leader whose log is not ready leaves the write quorum instead of keeping a stale ready slot.</summary>
    [Test]
    public async Task LeaderWithLogNotReadyLeavesQuorum()
    {
        var eligibility = new ReplicaEligibility(3);
        var ready = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 3, 1, 3, 0, FollowerLogReadiness.Ready, GroupMemberState.Complete, false);
        var failed = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 3, 1, 3, 0, FollowerLogReadiness.Failed, GroupMemberState.Complete, false);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in ready, Fingerprint, 1);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in failed, Fingerprint, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
    }

    /// <summary>Unverifying forgets the progress of a catching-up follower and takes a ready one out of the quorum.</summary>
    [Test]
    public async Task UnverifyForgetsCatchingUpFollower()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 4, 1, 4, 4, FollowerLogReadiness.Ready, GroupMemberState.Complete, false);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, Fingerprint, 1);
        var hint = new ReplicaProgress(8, 7, 0, 0, 0, Fingerprint, 1, 0);
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in hint)).IsTrue();
        var ready = new ReplicaProgress(5, 4, 4, 0, 1, Fingerprint, 1, 0);
        _ = await Assert.That(eligibility.TryMarkReady(2, in ready, in ready)).IsTrue();

        ReplicaReadinessProbe.UnverifyFollowers(eligibility, 0);

        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(1)).IsEqualTo(default);
        _ = await Assert.That(eligibility.StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(2)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }
}
