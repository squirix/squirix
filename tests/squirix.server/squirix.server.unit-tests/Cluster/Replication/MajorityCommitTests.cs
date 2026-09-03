using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Majority calculation and contiguous acknowledgement invariants.</summary>
[Immutable]
public sealed class MajorityCommitTests
{
    /// <summary>Ahead-of-prefix indexes are buffered and count once the missing prefix lands.</summary>
    [Fact]
    public void CommitIndexNeverSkipsMissingPrefix()
    {
        var quorum = new ReplicaCommitQuorum(3);
        var first = CreateMutation(1, [1]);
        var second = CreateMutation(2, [2]);

        Assert.True(quorum.TryRecord(0, CreateAcknowledgement(first), first));
        Assert.True(quorum.TryRecord(1, CreateAcknowledgement(second), second));
        Assert.True(quorum.TryRecord(2, CreateAcknowledgement(second), second));
        Assert.Equal(0UL, quorum.FindCommitIndex(0, 2));
        Assert.Equal(0UL, quorum.MatchIndexFor(1));

        Assert.True(quorum.TryRecord(1, CreateAcknowledgement(first), first));
        Assert.Equal(2UL, quorum.MatchIndexFor(1));
        Assert.Equal(1UL, quorum.FindCommitIndex(0, 2));
    }

    /// <summary>Every acknowledgement identity field must match the prepared mutation.</summary>
    [Fact]
    public void RejectsAcknowledgementForDifferentEntry()
    {
        var mutation = CreateMutation(1, [1, 2, 3]);
        var quorum = new ReplicaCommitQuorum(3);
        var valid = CreateAcknowledgement(mutation);
        var wrongGroup = valid with { GroupId = "other" };
        var wrongTerm = valid with { Term = 2 };
        var wrongIndex = valid with { LogIndex = 2 };
        var wrongFingerprint = valid with { OperationFingerprint = new byte[] { 9 } };
        var wrongChecksum = valid with { PayloadChecksum = 99 };
        var notDurable = valid with { IsDurable = false };
        var notReady = valid with { IsReady = false };

        Assert.False(quorum.TryRecord(0, in wrongGroup, mutation));
        Assert.False(quorum.TryRecord(0, in wrongTerm, mutation));
        Assert.False(quorum.TryRecord(0, in wrongIndex, mutation));
        Assert.False(quorum.TryRecord(0, in wrongFingerprint, mutation));
        Assert.False(quorum.TryRecord(0, in wrongChecksum, mutation));
        Assert.False(quorum.TryRecord(0, in notDurable, mutation));
        Assert.False(quorum.TryRecord(0, in notReady, mutation));
        Assert.Equal(0UL, quorum.MatchIndexFor(0));
    }

    /// <summary>Every supported replica factor uses the expected majority.</summary>
    [Fact]
    public void RequiredCopiesCoverAllConfiguredRfValues()
    {
        Assert.Equal(1, new ReplicaCommitQuorum(1).RequiredCopies);
        Assert.Equal(2, new ReplicaCommitQuorum(2).RequiredCopies);
        Assert.Equal(2, new ReplicaCommitQuorum(3).RequiredCopies);
        Assert.Equal(3, new ReplicaCommitQuorum(4).RequiredCopies);
        Assert.Equal(3, new ReplicaCommitQuorum(5).RequiredCopies);
    }

    /// <summary>Required copies use floor-half plus one.</summary>
    [Fact]
    public void RequiredCopiesUsesFloorHalfPlusOne()
    {
        Assert.Equal(2, new ReplicaCommitQuorum(3).RequiredCopies);
        Assert.Equal(3, new ReplicaCommitQuorum(5).RequiredCopies);
    }

    private static ReplicaDurableAcknowledgement CreateAcknowledgement(PreparedReplicaMutation mutation) => new(
        mutation.GroupId,
        mutation.Term,
        mutation.LogIndex,
        mutation.OperationFingerprint,
        mutation.PayloadChecksum,
        true,
        true);

    private static PreparedReplicaMutation CreateMutation(ulong index, byte[] fingerprint) => new(
        new ReplicaOperationIdentity("group-a", "client", "0123456789abcdef0123456789abcdef", fingerprint),
        1,
        index,
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 42),
        0);
}
