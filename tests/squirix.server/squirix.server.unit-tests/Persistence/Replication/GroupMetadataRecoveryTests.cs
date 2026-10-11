using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>What a follower log reads back from its two-slot metadata file: term, vote, and the refusal of files it cannot trust.</summary>
[Immutable]
public sealed class GroupMetadataRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "meta-recovery";

    /// <summary>A term step and a vote grant are read back by a reopen.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermAndVoteSurviveReopen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-reopen");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((4UL, "node-b"));
    }

    /// <summary>A vote grant is published into both slots, so each slot alone holds the term and the vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteGrantFillsBothSlots(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-both-slots");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);
        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

        for (var slot = 0; slot < 2; slot++)
        {
            var alone = new byte[bytes.Length];
            bytes.CopyTo(alone, 0);
            Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);

            var read = GroupMetadataSlots.Select(alone);

            _ = await Assert.That((read.Status, read.Slot, read.Metadata.CurrentTerm, read.Metadata.VotedFor)).IsEqualTo((GroupMetadataReadStatus.Valid, slot, 4UL, "node-b"));
        }
    }

    /// <summary>A grant for a candidate in the current term costs two metadata publishes, and a term step with it adds one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteGrantPublishesTwice(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-publishes");
        var hooks = new CountingFollowerLogFaultHooks();
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, new FollowerLogOptions { FaultHooks = hooks });
        await log.OpenAsync(cancellationToken);
        _ = await log.ObserveTermAsync(3UL, cancellationToken);

        var before = hooks.Snapshot();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 3UL, 0UL, 0UL), cancellationToken);
        var sameTerm = hooks.Snapshot().Since(in before);
        before = hooks.Snapshot();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), cancellationToken);
        var higherTerm = hooks.Snapshot().Since(in before);

        _ = await Assert.That((sameTerm.MetaWrites, higherTerm.MetaWrites)).IsEqualTo((2L, 3L));
    }

    /// <summary>
    /// Damage to either slot after a vote grant leaves the term and the vote in the other, so a reopen still holds the vote and a different
    /// candidate in the same term is refused.
    /// </summary>
    /// <param name="slot">The slot that is damaged.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task VoteSurvivesDamageToEitherSlot(int slot, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-damage");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);
        await FollowerLogTestKit.CorruptByteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), (slot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((4UL, "node-b"));
        var other = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That((other.Granted, other.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.AlreadyVoted));
    }

    /// <summary>
    /// A record that is safe to roll back, here a commit advance, falls back to the previous record when its newest slot is damaged, since an
    /// older commit index only means the entry is committed again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DamagedCommitAdvanceFallsBack(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-commit");
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        var newest = GroupMetadataStore.Read(path);
        _ = await Assert.That(newest.Metadata.CommitIndex).IsEqualTo(1UL);
        await FollowerLogTestKit.CorruptByteAsync(path, (newest.Slot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((reopened.Readiness, status.CommitIndex, status.LastLogIndex)).IsEqualTo((FollowerLogReadiness.Ready, 0UL, 1UL));
    }

    /// <summary>
    /// A fault on the second publish of a grant fails the operation without reporting the vote, and a reopen reads the first copy, so the vote
    /// is never lost to a different candidate.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMirrorPublishDoesNotReportGrant(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-mirror-fault");
        var publishes = 0;
        var failAt = int.MaxValue;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaPublishing().Callback(() =>
        {
            if (++publishes == failAt)
                throw new IOException("simulated failure at the mirror publish.");
        });

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults.Instance()))
        {
            await log.OpenAsync(cancellationToken);

            // The next publishes are the term step, the grant and its mirror.
            failAt = publishes + 3;
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.RequestVoteAsync(new ElectionVoteRequest("node-b", 4UL, 0UL, 0UL), cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).VotedFor).IsEqualTo(string.Empty);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((4UL, "node-b"));
        var other = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(other.Granted).IsFalse();
    }

    /// <summary>With both slots corrupt the log refuses to open and leaves the file untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BothSlotsCorruptFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-both");
        await GrantVoteAfterTermStepAsync(dir, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await FollowerLogTestKit.CorruptByteAsync(path, 20, cancellationToken);
        await FollowerLogTestKit.CorruptByteAsync(path, GroupMetadataSlots.SlotSize + 20, cancellationToken);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A metadata file in the layout of an earlier build fails readiness with a message naming the cause, and is not modified.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierLayoutFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-recovery-earlier");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, string.Empty, 0UL, 0UL);
        var earlier = new byte[GroupLogCodec.ComputeMetaEncodedLength(in meta)];
        GroupLogCodec.EncodeMeta(in meta, earlier);
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);

        await using var log = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("earlier build", StringComparison.Ordinal);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(earlier, after);
    }

    /// <summary>Observes term three, then grants a vote in term four, which publishes the term step and the grant separately, and closes the log.</summary>
    /// <param name="dir">The persistence root.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the log is closed.</returns>
    private static async Task GrantVoteAfterTermStepAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
}
