using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaTransferHarness;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A state transfer at the receiver from begin to end: in which order it writes, and what it leaves behind when it fails.</summary>
[Immutable]
public sealed class ReplicaTransferFlowTests : ServerUnitTestBase
{
    private const string Transfer = "transfer-1";
    private const ulong LeaderTerm = 1UL;

    /// <summary>The group is wiped in bounded steps, the copy ends complete at the transferred index, the applier is reseeded there, and the next entry is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferCompletesCopyAtSnapshotIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-flow");
        await using var harness = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 2 });
        harness.Sink.Keys = 3;
        var first = new[] { Entry("k-1", 1), Entry("k-2", 2) };
        var second = new[] { Entry("k-3", 3) };
        var totals = RecordingStateSink.TotalsOf([.. first, .. second]);

        var sequence = await harness.BeginAndUploadAsync(Transfer, LeaderTerm, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, first, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, second, cancellationToken);
        var end = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That((end.Success, end.RefusalCode, end.More)).IsEqualTo((true, string.Empty, false));
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((GroupMemberState.Complete, 3UL, 3UL, 3UL));
        await SequenceAssert.EqualAsync(["enter", "wipe", "wipe", "write", "write", "seal", "reset", "leave"], harness.Sink.Calls, StringComparer.Ordinal);
        _ = await Assert.That(harness.Sink.ResetIndex).IsEqualTo(3UL);
        _ = await Assert.That(harness.Sink.Held).IsFalse();
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
        _ = await Assert.That(await harness.Registry.ApplySignalFor(GroupId).WaitAsync(TimeSpan.FromHours(1), harness.Clock, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled).Because("The apply loop is woken.");
        var next = await log.AppendAsync(TransferFixtures.AppendAt(4UL, "d"), cancellationToken);
        _ = await Assert.That((next.Success, next.LastLogIndex)).IsEqualTo((true, 4UL));
    }

    /// <summary>The incomplete record is durable in both slots before the first key is removed, and the complete record is written after the journal barrier.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IncompleteIsDurableBeforeFirstWipe(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-order");
        await using var harness = await OpenAsync(dir, cancellationToken);
        harness.Sink.Keys = 1;
        var entries = new[] { Entry("k-1", 1) };
        var totals = RecordingStateSink.TotalsOf(entries);

        var sequence = await harness.BeginAndUploadAsync(Transfer, LeaderTerm, cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var uploaded = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await Assert.That(TransferFixtures.SlotStates(uploaded)).IsEqualTo((GroupMemberState.Complete, GroupMemberState.Complete)).Because("The begin and the snapshot upload write nothing.");
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, entries, cancellationToken);
        _ = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(harness.Sink.SlotsAtFirstWipe).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete));
        _ = await Assert.That(harness.Sink.SlotsAtSeal).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete)).Because("The barrier comes before the complete record.");
        var completed = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await Assert.That(TransferFixtures.SlotStates(completed)).IsEqualTo((GroupMemberState.Complete, GroupMemberState.Complete));
    }

    /// <summary>A snapshot that does not match what the begin declared ends the transfer and leaves the copy complete, untouched, with nothing wiped.</summary>
    /// <param name="badChecksum">Whether the declared checksum is wrong; otherwise the declared index is.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task MalformedSnapshotLeavesCopyUntouched(bool badChecksum, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-malformed");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var image = TransferFixtures.Image();
        var declared = BaselineOf(image);
        declared = badChecksum ? declared with { SnapshotChecksum = declared.SnapshotChecksum ^ 1U } : declared with { Index = declared.Index + 1 };
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);

        _ = await harness.Receiver.BeginAsync(HeaderOf(Transfer, LeaderTerm), declared, cancellationToken);
        var half = image.Length / 2;
        _ = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 1, 0, image.AsMemory(0, half), cancellationToken);
        var last = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 2, half, image.AsMemory(half), cancellationToken);

        _ = await Assert.That((last.Success, last.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
        await SequenceAssert.EqualAsync(["enter", "leave"], harness.Sink.Calls, StringComparer.Ordinal);
        var again = await harness.Receiver.BeginAsync(HeaderOf("transfer-2", LeaderTerm), BaselineOf(image), cancellationToken);
        _ = await Assert.That(again.Success).IsTrue().Because("The group is free again.");
    }

    /// <summary>Chunks that reach beyond the declared length of the image end the transfer and leave the copy complete and untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChunksBeyondDeclaredLengthEndTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-overflow");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var image = TransferFixtures.Image();
        var half = image.Length / 2;
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await harness.Receiver.BeginAsync(HeaderOf(Transfer, LeaderTerm), BaselineOf(image), cancellationToken);
        _ = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 1, 0, image.AsMemory(0, half), cancellationToken);

        var last = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 2, half, new byte[image.Length - half + 1], cancellationToken);

        _ = await Assert.That((last.Success, last.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
        await SequenceAssert.EqualAsync(["enter", "leave"], harness.Sink.Calls, StringComparer.Ordinal);
    }

    /// <summary>A snapshot of another topology than the copy's is refused at the first wipe step, before the copy is marked and before any key is removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherTopologySnapshotIsRefusedBeforeWipe(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-other-topology");
        await using var harness = await OpenAsync(dir, cancellationToken);
        harness.Sink.Keys = 2;
        var foreign = new GroupSnapshot(
            GroupId,
            new byte[] { 1, 2, 3 },
            1UL,
            TransferFixtures.SnapshotTerm,
            TransferFixtures.SnapshotIndex,
            TransferFixtures.SnapshotIndex,
            [],
            new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var image = new byte[GroupSnapshotStore.ComputePublishedLength(in foreign, GroupSnapshotStore.DefaultMaxSnapshotBytes)];
        GroupSnapshotStore.EncodePublished(image, in foreign);
        var half = image.Length / 2;
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        _ = await harness.Receiver.BeginAsync(HeaderOf(Transfer, LeaderTerm), BaselineOf(image), cancellationToken);
        _ = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 1, 0, image.AsMemory(0, half), cancellationToken);
        _ = await harness.Receiver.SnapshotAsync(HeaderOf(Transfer, LeaderTerm), 2, half, image.AsMemory(half), cancellationToken);

        var wipe = await harness.Receiver.WipeAsync(HeaderOf(Transfer, LeaderTerm), 3, cancellationToken);

        _ = await Assert.That((wipe.Success, wipe.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        _ = await Assert.That(harness.Sink.Keys).IsEqualTo(2).Because("No key was removed.");
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
        await SequenceAssert.EqualAsync(["enter", "leave"], harness.Sink.Calls, StringComparer.Ordinal);
    }

    /// <summary>A count or a checksum that disagrees at the end ends the transfer, and the copy stays incomplete and refuses appends as incomplete.</summary>
    /// <param name="wrongCount">Whether the count is wrong; otherwise the checksum is.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task MismatchAtEndLeavesCopyIncomplete(bool wrongCount, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-mismatch");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var entries = new[] { Entry("k-1", 1) };
        var totals = RecordingStateSink.TotalsOf(entries);

        var sequence = await harness.BeginAndUploadAsync(Transfer, LeaderTerm, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, entries, cancellationToken);
        var end = wrongCount
            ? await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount + 1, totals.Crc, cancellationToken)
            : await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc ^ 1U, cancellationToken);

        _ = await Assert.That((end.Success, end.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        var append = await log.AppendAsync(TransferFixtures.AppendAt(3UL, "c"), cancellationToken);
        _ = await Assert.That((append.Success, append.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Incomplete));
        _ = await Assert.That(harness.Sink.Calls[^1]).IsEqualTo("leave");
        _ = await Assert.That(harness.Registry.TransferHoldFor(GroupId).IsHeld).IsFalse();
    }

    /// <summary>An entry batch the sink cannot decode ends the transfer as a checksum mismatch and leaves the copy incomplete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UndecodableBatchEndsTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-undecodable");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, LeaderTerm, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);

        var reply = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, new[] { new ReplicaStateEntry("cache", new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, 0L) }, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Sink.Held).IsFalse();
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
    }

    /// <summary>A crash after the first wipe step reopens an incomplete copy that takes no append, and a new transfer wipes again and completes it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrashMidTransferThenNewTransferCompletes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-crash");
        var crashed = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        crashed.Sink.Keys = 2;
        var sequence = await crashed.BeginAndUploadAsync(Transfer, LeaderTerm, cancellationToken);
        var step = await crashed.Receiver.WipeAsync(crashed.HeaderFor(Transfer), sequence, cancellationToken);
        _ = await Assert.That((step.Success, step.More)).IsEqualTo((true, true));
        await crashed.DisposeAsync();

        await using var reopened = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        reopened.Sink.Keys = 2;
        _ = await Assert.That(reopened.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        var refused = await log.AppendAsync(TransferFixtures.AppendAt(3UL, "c"), cancellationToken);
        _ = await Assert.That((refused.Success, refused.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Incomplete));
        var entries = new[] { Entry("k-1", 1) };
        var totals = RecordingStateSink.TotalsOf(entries);

        sequence = await reopened.BeginAndUploadAsync("transfer-2", LeaderTerm, cancellationToken);
        sequence = await reopened.WipeAllAsync("transfer-2", sequence, cancellationToken);
        sequence = await reopened.SendBatchAsync("transfer-2", sequence, entries, cancellationToken);
        var end = await reopened.Receiver.EndAsync(reopened.HeaderFor("transfer-2"), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(end.Success).IsTrue();
        _ = await Assert.That(log.IsComplete).IsTrue();
        _ = await Assert.That(reopened.Sink.Keys).IsEqualTo(0).Because("The new transfer wiped the keys again.");
    }
}
