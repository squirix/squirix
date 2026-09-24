using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Majority calculation and contiguous acknowledgement invariants.</summary>
[Immutable]
public sealed class MajorityCommitTests
{
    /// <summary>Ahead-of-prefix indexes are buffered and count once the missing prefix lands.</summary>
    [Test]
    public async Task CommitIndexNeverSkipsMissingPrefix()
    {
        var quorum = new ReplicaCommitQuorum(3);
        var first = CreateMutation(1, [1]);
        var second = CreateMutation(2, [2]);

        _ = await Assert.That(quorum.TryRecord(0, CreateAcknowledgement(first), first)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(second), second)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(2, CreateAcknowledgement(second), second)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0, 2)).IsEqualTo(0UL);
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(0UL);

        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(first), first)).IsTrue();
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(2UL);
        _ = await Assert.That(quorum.FindCommitIndex(0, 2)).IsEqualTo(1UL);
    }

    /// <summary>Every acknowledgement identity field must match the prepared mutation.</summary>
    [Test]
    public async Task RejectsAcknowledgementForDifferentEntry()
    {
        var mutation = CreateMutation(1, [1, 2, 3]);
        var quorum = new ReplicaCommitQuorum(3);
        var valid = CreateAcknowledgement(mutation);
        var wrongGroup = valid with { GroupId = "other" };
        var wrongTerm = valid with { Term = 2 };
        var wrongIndex = valid with { LogIndex = 2 };
        var wrongFingerprint = valid with { OperationFingerprint = Array.Empty<byte>() };
        var wrongChecksum = valid with { PayloadChecksum = 99 };
        var notDurable = valid with { IsDurable = false };
        var notReady = valid with { IsReady = false };

        _ = await Assert.That(quorum.TryRecord(0, in wrongGroup, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in wrongTerm, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in wrongIndex, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in wrongFingerprint, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in wrongChecksum, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in notDurable, mutation)).IsFalse();
        _ = await Assert.That(quorum.TryRecord(0, in notReady, mutation)).IsFalse();
        _ = await Assert.That(quorum.MatchIndexFor(0)).IsEqualTo(0UL);
    }

    /// <summary>A slot admitted at a verified index counts its next contiguous acknowledgement.</summary>
    [Test]
    public async Task AdmittedReplicaCountsFromVerifiedIndex()
    {
        var eligibility = new ReplicaEligibility(3);
        var quorum = new ReplicaCommitQuorum(3, 2, eligibility);
        var ready = new ReplicaProgress(4, 3, 3, 0, 1, ReadOnlyMemory<byte>.Of(9), 1, 0);
        var mutation = CreateMutation(4, [4]);
        quorum.Admit(0, 3);
        quorum.Admit(1, 3);
        _ = await Assert.That(eligibility.TryMarkReady(0, in ready, in ready)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkReady(1, in ready, in ready)).IsTrue();

        _ = await Assert.That(quorum.TryRecord(0, CreateAcknowledgement(mutation), mutation)).IsTrue();
        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(mutation), mutation)).IsTrue();

        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(4UL);
        _ = await Assert.That(quorum.FindCommitIndex(3, 4)).IsEqualTo(4UL);
    }

    /// <summary>Admission never lowers a match index and folds buffered acknowledgements it now covers.</summary>
    [Test]
    public async Task AdmitNeverRewindsMatchIndex()
    {
        var quorum = new ReplicaCommitQuorum(3);
        var ahead = CreateMutation(5, [5]);
        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(ahead), ahead)).IsTrue();
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(0UL);

        quorum.Admit(1, 4);
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(5UL);

        quorum.Admit(1, 2);
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(5UL);
    }

    /// <summary>Every supported replica factor uses the expected majority.</summary>
    [Test]
    public async Task RequiredCopiesCoverAllConfiguredRfValues()
    {
        _ = await Assert.That(new ReplicaCommitQuorum(1).RequiredCopies).IsEqualTo(1);
        _ = await Assert.That(new ReplicaCommitQuorum(2).RequiredCopies).IsEqualTo(2);
        _ = await Assert.That(new ReplicaCommitQuorum(3).RequiredCopies).IsEqualTo(2);
        _ = await Assert.That(new ReplicaCommitQuorum(4).RequiredCopies).IsEqualTo(3);
        _ = await Assert.That(new ReplicaCommitQuorum(5).RequiredCopies).IsEqualTo(3);
    }

    /// <summary>Required copies use floor-half plus one.</summary>
    [Test]
    public async Task RequiredCopiesUsesFloorHalfPlusOne()
    {
        _ = await Assert.That(new ReplicaCommitQuorum(3).RequiredCopies).IsEqualTo(2);
        _ = await Assert.That(new ReplicaCommitQuorum(5).RequiredCopies).IsEqualTo(3);
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
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 42));
}
