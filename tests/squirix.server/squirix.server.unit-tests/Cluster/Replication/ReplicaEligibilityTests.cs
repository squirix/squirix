using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Replica participation gates for recovery, quorum, voting, and promotion.</summary>
[Immutable]
public sealed class ReplicaEligibilityTests
{
    /// <summary>A stale participant cannot exercise authority or contribute a durable copy before catch-up verification.</summary>
    [Fact(DisplayName = "Squirix.Server.UnitTests.Cluster.Replication.ReplicaEligibilityTests.StaleReplicaHasNoVotePromotionOrWriteQuorum")]
    public void StaleReplicaHasNoAuthority()
    {
        var eligibility = new ReplicaEligibility(3);
        var target = Progress(2, 1, 1, 1, 7, 42);
        Assert.True(eligibility.TryMarkReady(0, in target, in target));
        Assert.True(eligibility.TryMarkCatchingUp(1, in target));

        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        Assert.True(quorum.TryRecord(0, in acknowledgement, mutation));
        Assert.False(quorum.TryRecord(1, in acknowledgement, mutation));
        Assert.False(eligibility.CanVote(1));
        Assert.False(eligibility.CanBePromoted(1));
        Assert.False(eligibility.CanCountInWriteQuorum(1));
        Assert.Equal(0UL, quorum.FindCommitIndex(0, 1));

        Assert.True(eligibility.TryMarkReady(1, in target, in target));
        Assert.True(quorum.TryRecord(1, in acknowledgement, mutation));
        Assert.Equal(1UL, quorum.FindCommitIndex(0, 1));
    }

    /// <summary>Every participant begins recovering with no retained progress.</summary>
    [Fact]
    public void NewParticipantsBeginRecovering()
    {
        var eligibility = new ReplicaEligibility(3);

        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            Assert.Equal(ReplicaParticipantState.Recovering, eligibility.StateFor(i));
            Assert.False(eligibility.CanCountInWriteQuorum(i));
            Assert.Equal(default, eligibility.ProgressFor(i));
        }
    }

    /// <summary>Every named readiness state has a stable distinct value.</summary>
    [Fact]
    public void ParticipationStatesAreExplicit()
    {
        ReplicaParticipantState[] expected =
        [
            ReplicaParticipantState.Recovering,
            ReplicaParticipantState.CatchingUp,
            ReplicaParticipantState.Ready,
            ReplicaParticipantState.Quarantined,
        ];

        Assert.Equal(expected, Enum.GetValues<ReplicaParticipantState>());
    }

    /// <summary>Readiness requires exact identity, generation, indexes, term, and checksum.</summary>
    [Fact]
    public void ReadinessRequiresVerifiedProgress()
    {
        var expected = Progress(5, 4, 4, 4, 3, 91);
        ReplicaProgress[] mismatches =
        [
            expected with { NextIndex = 4 },
            expected with { MatchIndex = 3, NextIndex = 4, CommitIndex = 3, AppliedIndex = 3 },
            expected with { CommitIndex = 3 },
            expected with { AppliedIndex = 3 },
            expected with { LastTerm = 2 },
            expected with { TopologyFingerprint = new byte[] { 9, 9 } },
            expected with { ConfigurationGeneration = 8 },
            expected with { StateChecksum = 92 },
        ];

        foreach (var mismatch in mismatches)
        {
            var eligibility = new ReplicaEligibility(1);
            Assert.False(eligibility.TryMarkReady(0, in mismatch, in expected));
            Assert.False(eligibility.CanCountInWriteQuorum(0));
        }
    }

    /// <summary>A progress regression removes a previously ready durable copy from commit calculation.</summary>
    [Fact]
    public void ProgressRegressionRemovesReadyCopy()
    {
        var eligibility = new ReplicaEligibility(3);
        var current = Progress(2, 1, 1, 1, 1, 10);
        Assert.True(eligibility.TryMarkReady(0, in current, in current));
        Assert.True(eligibility.TryMarkReady(1, in current, in current));
        var quorum = new ReplicaCommitQuorum(3, eligibility: eligibility);
        var mutation = Mutation(1);
        var acknowledgement = Acknowledgement(mutation);
        Assert.True(quorum.TryRecord(0, in acknowledgement, mutation));
        Assert.True(quorum.TryRecord(1, in acknowledgement, mutation));
        Assert.Equal(1UL, quorum.FindCommitIndex(0, 1));

        var regressed = Progress(1, 0, 0, 0, 1, 11);
        Assert.False(eligibility.TryMarkCatchingUp(1, in regressed));
        Assert.Equal(ReplicaParticipantState.CatchingUp, eligibility.StateFor(1));
        Assert.Equal(0UL, quorum.FindCommitIndex(0, 1));
    }

    /// <summary>An invalid catch-up report demotes a ready participant and revokes authority.</summary>
    [Fact]
    public void InvalidCatchUpDemotesReadyParticipant()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(2, 1, 1, 1, 7, 42);
        Assert.True(eligibility.TryMarkReady(0, in target, in target));
        var invalid = target with { NextIndex = 99 };

        Assert.False(eligibility.TryMarkCatchingUp(0, in invalid));
        Assert.Equal(ReplicaParticipantState.CatchingUp, eligibility.StateFor(0));
        Assert.Equal(default, eligibility.ProgressFor(0));
        Assert.False(eligibility.CanVote(0));
        Assert.False(eligibility.CanBePromoted(0));
        Assert.False(eligibility.CanCountInWriteQuorum(0));
    }

    /// <summary>An invalid readiness report demotes a ready participant and revokes authority.</summary>
    [Fact]
    public void InvalidReadinessDemotesReadyParticipant()
    {
        var eligibility = new ReplicaEligibility(1);
        var target = Progress(2, 1, 1, 1, 7, 42);
        Assert.True(eligibility.TryMarkReady(0, in target, in target));
        var invalid = target with { AppliedIndex = 99 };

        Assert.False(eligibility.TryMarkReady(0, in invalid, in target));
        Assert.Equal(ReplicaParticipantState.CatchingUp, eligibility.StateFor(0));
        Assert.Equal(default, eligibility.ProgressFor(0));
        Assert.False(eligibility.CanVote(0));
        Assert.False(eligibility.CanBePromoted(0));
        Assert.False(eligibility.CanCountInWriteQuorum(0));
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
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4),
        0);

    private static ReplicaProgress Progress(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong appliedIndex, ulong generation, uint checksum) =>
        new(nextIndex, matchIndex, commitIndex, appliedIndex, 1, new byte[] { 1, 2, 3 }, generation, checksum);
}
