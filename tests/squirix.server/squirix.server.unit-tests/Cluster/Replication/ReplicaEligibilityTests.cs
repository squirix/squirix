using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Replica participation gates for recovery, quorum, voting, and promotion.</summary>
[Immutable]
public sealed class ReplicaEligibilityTests
{
    /// <summary>An invalid catch-up report demotes a ready participant and revokes authority.</summary>
    [Test]
    public async Task InvalidCatchUpDemotesReadyParticipant()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(2, 1, 1, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();
        var invalid = target with { NextIndex = 99 };

        _ = await Assert.That(eligibility.TryMarkCatchingUp(0, in invalid)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanVote(0)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
    }

    /// <summary>An invalid readiness report demotes a ready participant and revokes authority.</summary>
    [Test]
    public async Task InvalidReadinessDemotesReadyParticipant()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(2, 1, 1, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();
        var invalid = target with { CommitIndex = 99 };

        _ = await Assert.That(eligibility.TryMarkReady(0, in invalid)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanVote(0)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
    }

    /// <summary>A ready participant that did not acknowledge an entry past its verified progress is demoted and loses its progress.</summary>
    [Test]
    public async Task DemoteLeavesReadyPastProgress()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(5, 4, 4, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();

        _ = await Assert.That(eligibility.TryDemote(0, 5)).IsTrue();

        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.TryDemote(0, 6)).IsFalse();
    }

    /// <summary>A failure for an index the participant was verified to hold, or for a participant that is not ready, changes nothing.</summary>
    [Test]
    public async Task StaleOrNotReadyIsNotDemoted()
    {
        var eligibility = new ReplicaEligibility(2);
        var target = Progress(5, 4, 4, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();

        _ = await Assert.That(eligibility.TryDemote(0, 4)).IsFalse();
        _ = await Assert.That(eligibility.TryDemote(1, 9)).IsFalse();

        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.Recovering);
    }

    /// <summary>Forgetting a ready participant drops its progress and its place in the quorum, and a later lower report is accepted as ready.</summary>
    [Test]
    public async Task ForgetDropsProgressAndQuorumPlace()
    {
        var eligibility = new ReplicaEligibility(1);
        var high = Progress(9, 8, 8, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in high)).IsTrue();

        eligibility.Forget(0);

        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();

        var low = Progress(3, 2, 2, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in low)).IsTrue();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

    /// <summary>Forgetting a catching-up participant clears its floor and keeps it catching up.</summary>
    [Test]
    public async Task ForgetClearsFloor()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(5, 4, 4, 1, 7);
        _ = await Assert.That(eligibility.TryMarkCatchingUp(0, in target)).IsTrue();

        eligibility.Forget(0);

        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
    }

    /// <summary>Every participant begins recovering with no retained progress.</summary>
    [Test]
    public async Task NewParticipantsBeginRecovering()
    {
        var eligibility = new ReplicaEligibility(3);

        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            _ = await Assert.That(eligibility.StateFor(i)).IsEqualTo(ReplicaParticipantState.Recovering);
            _ = await Assert.That(eligibility.CanCountInWriteQuorum(i)).IsFalse();
            _ = await Assert.That(eligibility.ProgressFor(i)).IsEqualTo(default);
        }
    }

    /// <summary>Every named readiness state has a stable distinct value.</summary>
    [Test]
    public Task ParticipationStatesAreExplicit()
    {
        ReplicaParticipantState[] expected =
        [
            ReplicaParticipantState.Recovering,
            ReplicaParticipantState.CatchingUp,
            ReplicaParticipantState.Ready,
        ];

        return SequenceAssert.EqualAsync(expected, Enum.GetValues<ReplicaParticipantState>());
    }

    /// <summary>A progress regression removes a previously ready durable copy from commit calculation.</summary>
    [Test]
    public async Task ProgressRegressionRemovesReadyCopy()
    {
        var eligibility = new ReplicaEligibility(3);
        var current = Progress(2, 1, 1, 1, 1);
        _ = await Assert.That(eligibility.TryMarkReady(0, in current)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkReady(1, in current)).IsTrue();
        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        _ = await Assert.That(quorum.TryRecord(0, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(1UL);

        var regressed = Progress(1, 0, 0, 1, 1);
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in regressed)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(0UL);
    }

    /// <summary>A structurally invalid report never makes a participant ready.</summary>
    [Test]
    public async Task InvalidReportIsNeverReady()
    {
        var valid = Progress(5, 4, 4, 1, 3);
        ReplicaProgress[] invalid =
        [
            valid with { NextIndex = 4 },
            valid with { CommitIndex = 5 },
            valid with { TopologyFingerprint = default },
            valid with { ConfigurationGeneration = 0 },
            valid with { MatchIndex = ulong.MaxValue, NextIndex = 0 },
        ];

        foreach (var report in invalid)
        {
            var eligibility = new ReplicaEligibility(1);
            _ = await Assert.That(eligibility.TryMarkReady(0, in report)).IsFalse();
            _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
        }
    }

    /// <summary>A report with one field below the recorded floor of a catching-up participant keeps it catching up.</summary>
    /// <param name="nextIndex">Next index of the lower report.</param>
    /// <param name="matchIndex">Match index of the lower report.</param>
    /// <param name="commitIndex">Commit index of the lower report.</param>
    /// <param name="lastTerm">Last term of the lower report.</param>
    [Test]
    [Arguments(4UL, 3UL, 2UL, 3UL)]
    [Arguments(5UL, 4UL, 1UL, 3UL)]
    [Arguments(5UL, 4UL, 2UL, 2UL)]
    public async Task LowerReadinessStaysCatchingUp(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong lastTerm)
    {
        var eligibility = new ReplicaEligibility(1);
        var floor = Progress(5, 4, 2, 3, 1);
        _ = await Assert.That(eligibility.TryMarkCatchingUp(0, in floor)).IsTrue();

        var lower = Progress(nextIndex, matchIndex, commitIndex, lastTerm, 1);
        _ = await Assert.That(eligibility.TryMarkReady(0, in lower)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();

        _ = await Assert.That(eligibility.TryMarkReady(0, in floor)).IsTrue();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

    /// <summary>A stale participant cannot exercise authority or contribute a durable copy before catch-up verification.</summary>
    [Test]
    public async Task StaleReplicaHasNoAuthority()
    {
        var eligibility = new ReplicaEligibility(3);
        var target = Progress(2, 1, 1, 1, 7);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in target)).IsTrue();

        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        _ = await Assert.That(quorum.TryRecord(0, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsFalse();
        _ = await Assert.That(eligibility.CanVote(1)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(0UL);

        _ = await Assert.That(eligibility.TryMarkReady(1, in target)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(1UL);
    }

    /// <summary>The write majority needs more than half of the slots verified ready.</summary>
    [Test]
    public async Task WriteMajorityNeedsMoreThanHalfReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var target = Progress(2, 1, 1, 1, 7);
        _ = await Assert.That(eligibility.HasWriteMajority()).IsFalse();

        _ = await Assert.That(eligibility.TryMarkReady(0, in target)).IsTrue();
        _ = await Assert.That(eligibility.HasWriteMajority()).IsFalse();

        _ = await Assert.That(eligibility.TryMarkReady(2, in target)).IsTrue();
        _ = await Assert.That(eligibility.HasWriteMajority()).IsTrue();
    }

    private static ReplicaDurableAcknowledgement Acknowledgement(PreparedReplicaMutation mutation) => new(
        mutation.GroupId,
        mutation.Term,
        mutation.LogIndex,
        mutation.OperationFingerprint,
        mutation.PayloadChecksum,
        true,
        true);

    private static PreparedReplicaMutation Mutation(ulong index) => new(
        new ReplicaOperationIdentity("group-a", "client", "0123456789abcdef0123456789abcdef", new byte[] { 1 }),
        1,
        index,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));

    private static ReplicaProgress Progress(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong lastTerm, ulong generation) => new(
        nextIndex,
        matchIndex,
        commitIndex,
        lastTerm,
        new byte[] { 1, 2, 3 },
        generation);
}
