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
        var target = Progress(2, 1, 1, 1, 7, 42);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target, in target)).IsTrue();
        var invalid = target with { NextIndex = 99 };

        _ = await Assert.That(eligibility.TryMarkCatchingUp(0, in invalid)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanVote(0)).IsFalse();
        _ = await Assert.That(eligibility.CanBePromoted(0)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
    }

    /// <summary>An invalid readiness report demotes a ready participant and revokes authority.</summary>
    [Test]
    public async Task InvalidReadinessDemotesReadyParticipant()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(2, 1, 1, 1, 7, 42);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target, in target)).IsTrue();
        var invalid = target with { AppliedIndex = 99 };

        _ = await Assert.That(eligibility.TryMarkReady(0, in invalid, in target)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(0)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.ProgressFor(0)).IsEqualTo(default);
        _ = await Assert.That(eligibility.CanVote(0)).IsFalse();
        _ = await Assert.That(eligibility.CanBePromoted(0)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
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
            ReplicaParticipantState.Quarantined,
        ];

        return SequenceAssert.Equal(expected, Enum.GetValues<ReplicaParticipantState>());
    }

    /// <summary>A progress regression removes a previously ready durable copy from commit calculation.</summary>
    [Test]
    public async Task ProgressRegressionRemovesReadyCopy()
    {
        var eligibility = new ReplicaEligibility(3);
        var current = Progress(2, 1, 1, 1, 1, 10);
        _ = await Assert.That(eligibility.TryMarkReady(0, in current, in current)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkReady(1, in current, in current)).IsTrue();
        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        _ = await Assert.That(quorum.TryRecord(0, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(1UL);

        var regressed = Progress(1, 0, 0, 0, 1, 11);
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in regressed)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(0UL);
    }

    /// <summary>Readiness requires exact identity, generation, indexes, term, and checksum.</summary>
    [Test]
    public async Task ReadinessRequiresVerifiedProgress()
    {
        var expected = Progress(5, 4, 4, 4, 3, 91);
        ReplicaProgress[] mismatches =
        [
            expected with { NextIndex = 4 },
            expected with { MatchIndex = 3, NextIndex = 4, CommitIndex = 3, AppliedIndex = 3 },
            expected with { CommitIndex = 3 },
            expected with { AppliedIndex = 3 },
            expected with { LastTerm = 2 },
            expected with { TopologyFingerprint = new byte[] { 8, 8 } },
            expected with { ConfigurationGeneration = 8 },
            expected with { StateChecksum = 92 },
        ];

        foreach (var mismatch in mismatches)
        {
            var eligibility = new ReplicaEligibility(1);
            _ = await Assert.That(eligibility.TryMarkReady(0, in mismatch, in expected)).IsFalse();
            _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsFalse();
        }
    }

    /// <summary>A stale participant cannot exercise authority or contribute a durable copy before catch-up verification.</summary>
    [Test]
    public async Task StaleReplicaHasNoAuthority()
    {
        var eligibility = new ReplicaEligibility(3);
        var target = Progress(2, 1, 1, 1, 7, 42);
        _ = await Assert.That(eligibility.TryMarkReady(0, in target, in target)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in target)).IsTrue();

        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        _ = await Assert.That(quorum.TryRecord(0, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsFalse();
        _ = await Assert.That(eligibility.CanVote(1)).IsFalse();
        _ = await Assert.That(eligibility.CanBePromoted(1)).IsFalse();
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(0UL);

        _ = await Assert.That(eligibility.TryMarkReady(1, in target, in target)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, in acknowledgement, mutation)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0, 1)).IsEqualTo(1UL);
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

    private static ReplicaProgress Progress(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong appliedIndex, ulong generation, uint checksum) => new(
        nextIndex,
        matchIndex,
        commitIndex,
        appliedIndex,
        1,
        new byte[] { 1, 2, 3 },
        generation,
        checksum);
}
