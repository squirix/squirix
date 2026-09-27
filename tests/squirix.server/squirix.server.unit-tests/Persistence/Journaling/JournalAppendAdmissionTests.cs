using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Append admission (issue #703): a plain append has no write ack, so a frame the journal thread might reject for capacity is refused to
/// its caller before it enters the ring, from producer-side reads of the journal thread's counters. The active segment byte counter must
/// therefore equal what the journal thread will see once it opens the segment, even before that open.
/// </summary>
[Immutable]
public sealed class JournalAppendAdmissionTests : IsolatedStorageTestBase
{
    private const int BacklogPayload = 64 * 1024;

    private const int FillPayload = 64 * 1024;

    private const long MaxBytes = 1024L * 1024L;

    private const int SmallPayload = 16;

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Appends still ahead of a frame count toward its admission: a frame that fits what the journal holds now but not what it will hold
    /// once the backlog lands is refused, exactly the frame the journal thread would otherwise reject after the backlog was written.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BacklogCountsTowardAdmission(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, 1, cancellationToken);
        await AppendDurablyAsync(journal, "w", SmallPayload, cancellationToken);
        journal.Writer.Write.Arm();
        for (var i = 0; i < 4; i++)
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default($"b{i}"), new byte[BacklogPayload], cancellationToken);

        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var usedBeforeBacklog = journal.Journal.UsedBytes;
        var pendingCount = journal.Journal.PendingAppends.PendingCount;
        var bigPayload = Convert.ToInt32(MaxBytes - usedBeforeBacklog - BacklogPayload);
        var bigFrame = FrameLength("big", bigPayload);

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(
            journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("big"), new byte[bigPayload], cancellationToken));
        journal.Writer.Write.Release();
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        var usedAfterBacklog = journal.Journal.UsedBytes;
        var failed = journal.Journal.HasFlushLoopFailure;
        await journal.ShutdownAsync();

        _ = await Assert.That(pendingCount).IsEqualTo(4);
        _ = await Assert.That(usedBeforeBacklog + bigFrame + JournalFraming.FileHeaderSize).IsLessThanOrEqualTo(MaxBytes);
        _ = await Assert.That(usedAfterBacklog + bigFrame).IsGreaterThan(MaxBytes);
        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("b0", "b1", "b2", "b3", "w"));
    }

    /// <summary>
    /// Many small frames queued behind a stalled write near a segment end count as one possible roll, not one each: with eight segments
    /// allowed, the frame that needs a roll behind forty of them is admitted, the journal rolls once, and the pipeline stays healthy.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BacklogNearSegmentEndStillRolls(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, JournalSegmentLimits.DefaultMaxTotalBytesMb, 1, 8, cancellationToken);
        await FillSegmentAsync(journal, cancellationToken);
        journal.Writer.Write.Arm();
        var keys = new string[42];
        keys[0] = "fill";
        keys[1] = "roll";
        for (var i = 0; i < 40; i++)
        {
            keys[i + 2] = $"s{i}";
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default(keys[i + 2]), new byte[SmallPayload], cancellationToken);
        }

        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var pendingCount = journal.Journal.PendingAppends.PendingCount;
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("roll"), new byte[(2 * FillPayload) + 1024], cancellationToken);
        journal.Writer.Write.Release();
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        var segmentIndex = journal.Journal.CurrentSegmentIndex;
        var failed = journal.Journal.HasFlushLoopFailure;
        await journal.ShutdownAsync();

        _ = await Assert.That(pendingCount).IsEqualTo(40);
        _ = await Assert.That(segmentIndex).IsEqualTo(2);
        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(2);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys(keys));
    }

    /// <summary>
    /// A frame refused for capacity burns its sequence without writing it; a snapshot cut and a restart still place the next frame above the
    /// snapshot watermark, so it replays.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BurnedSequenceStaysBelowRestart(CancellationToken cancellationToken)
    {
        ulong watermark;
        await using (var first = await StallableJournal.CreateAsync(Dir, false, 1, cancellationToken))
        {
            await AppendDurablyAsync(first, "a", SmallPayload, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(
                first.Journal.AppendPutUnderGateAsync(CacheKey.Default("big"), new byte[MaxBytes + 1024L], cancellationToken));
            watermark = await first.Journal.ExecuteSnapshotCutAsync(
                0,
                static (_, sequence, _) => ValueTask.FromResult(sequence),
                static (_, sequence, _, _) => ValueTask.FromResult(sequence),
                cancellationToken);
            await first.Ledger.WriteAsync(new State { CurrentJournal = 1, LastSnapshot = new SnapshotRef { CreatedUtc = DateTime.UtcNow, Index = 1, LastAppliedSequence = watermark } }, cancellationToken);
        }

        await using var restarted = await StallableJournal.CreateAsync(Dir, false, 1, cancellationToken);
        var nextSequence = restarted.Journal.NextSequence;
        await AppendDurablyAsync(restarted, "c", SmallPayload, cancellationToken);
        await restarted.ShutdownAsync();

        _ = await Assert.That(nextSequence).IsEqualTo(watermark + 1UL);
        _ = await Assert.That(restarted.Recover(string.Empty, watermark, cancellationToken)).IsEqualTo(Keys("c"));
    }

    /// <summary>A fresh journal seeds the active segment counter with the header the first open writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshStartSeedsHeaderBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var seeded = journal.Journal.ActiveSegmentWrittenBytes;

        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(seeded).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
    }

    /// <summary>
    /// After a maintenance end, the reset segment is not open yet: a frame that would roll it at the segment count limit is refused at
    /// admission (the pipeline keeps running), and a smaller frame that fits is written.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceEndRefusesRollAtCountLimit(CancellationToken cancellationToken)
    {
        await using var journal = await CreateSingleSegmentJournalAsync(cancellationToken);
        await FillSegmentAsync(journal, cancellationToken);

        await journal.Journal.ExecuteMaintenanceExclusiveAsync(static _ => ValueTask.CompletedTask, cancellationToken);
        var failed = await RefuseRollThenFitAsync(journal, cancellationToken);
        await journal.ShutdownAsync();

        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("fill", "fits"));
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(1);
    }

    /// <summary>After a no-op maintenance, the active segment counter holds the reset segment's length instead of zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceEndSeedsSegmentBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);

        await journal.Journal.ExecuteMaintenanceExclusiveAsync(static _ => ValueTask.CompletedTask, cancellationToken);

        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsEqualTo(SegmentLength(1));
        _ = await Assert.That(journal.Journal.ActiveSegmentWrittenBytes).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    /// <summary>
    /// A frame that does not fit even an empty segment is refused at admission instead of rolling segment after segment on the journal
    /// thread: the journal keeps exactly its one segment and accepts the next frame.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NeverFittingFrameRefused(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(
            Dir,
            false,
            JournalSegmentLimits.DefaultMaxTotalBytesMb,
            1,
            JournalSegmentLimits.DefaultMaxSegmentCount,
            cancellationToken);
        await AppendDurablyAsync(journal, "a", SmallPayload, cancellationToken);

        var refused = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(
            journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("big"), new byte[MaxBytes + 1024L], cancellationToken));
        await AppendDurablyAsync(journal, "b", SmallPayload, cancellationToken);
        var failed = journal.Journal.HasFlushLoopFailure;
        await journal.ShutdownAsync();

        _ = await Assert.That(refused.Message).Contains("segment size");
        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(1);
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("a", "b"));
    }

    /// <summary>
    /// A restart after a crash between a roll target's pre-creation and its manifest publish: the header-only target is already counted, so
    /// at the segment count limit the frame that rolls into it is admitted, the journal rolls without adding a segment, and the pipeline
    /// stays healthy.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRollsIntoCountedTarget(CancellationToken cancellationToken)
    {
        await using (var first = await StallableJournal.CreateAsync(Dir, false, JournalSegmentLimits.DefaultMaxTotalBytesMb, 1, 2, cancellationToken))
            await FillSegmentAsync(first, cancellationToken);

        WriteHeaderOnlySegment(JournalReadPath.BuildSegmentPath(Dir, 2));

        await using var restarted = await StallableJournal.CreateAsync(Dir, false, JournalSegmentLimits.DefaultMaxTotalBytesMb, 1, 2, cancellationToken);
        await AppendDurablyAsync(restarted, "roll", (2 * FillPayload) + 1024, cancellationToken);
        var segmentIndex = restarted.Journal.CurrentSegmentIndex;
        var segmentCount = restarted.Journal.EventLoop.JournalSegmentCount;
        var failed = restarted.Journal.HasFlushLoopFailure;
        await restarted.ShutdownAsync();

        _ = await Assert.That(segmentIndex).IsEqualTo(2);
        _ = await Assert.That(segmentCount).IsEqualTo(2);
        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(restarted.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("fill", "roll"));
    }

    /// <summary>
    /// A restart over a nearly full current segment at the segment count limit: the frame that would roll is refused at admission (the
    /// pipeline keeps running) before any append opened the segment, and a smaller frame that fits is written.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRefusesRollAtCountLimit(CancellationToken cancellationToken)
    {
        await using (var first = await CreateSingleSegmentJournalAsync(cancellationToken))
            await FillSegmentAsync(first, cancellationToken);

        await using var restarted = await CreateSingleSegmentJournalAsync(cancellationToken);
        var failed = await RefuseRollThenFitAsync(restarted, cancellationToken);
        await restarted.ShutdownAsync();

        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(restarted.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("fill", "fits"));
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(1);
    }

    /// <summary>A restart seeds the active segment counter with the current segment's on-disk length before any append opens it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartSeedsSegmentBytes(CancellationToken cancellationToken)
    {
        await using (var first = await StallableJournal.CreateAsync(Dir, false, cancellationToken))
        {
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
            await first.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
            await first.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var onDisk = SegmentLength(1);
        await using var restarted = await StallableJournal.CreateAsync(Dir, false, cancellationToken);

        _ = await Assert.That(restarted.Journal.ActiveSegmentWrittenBytes).IsEqualTo(onDisk);
        _ = await Assert.That(onDisk).IsGreaterThan(JournalFraming.FileHeaderSize);
    }

    /// <summary>
    /// With the only allowed segment nearly full, a frame that needs a roll is refused at admission, a smaller frame that fits is written,
    /// and only the accepted frames replay.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SegmentCountLimitRefusesRoll(CancellationToken cancellationToken)
    {
        await using var journal = await CreateSingleSegmentJournalAsync(cancellationToken);
        await FillSegmentAsync(journal, cancellationToken);

        var failed = await RefuseRollThenFitAsync(journal, cancellationToken);
        await journal.ShutdownAsync();

        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("fill", "fits"));
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(1);
    }

    /// <summary>
    /// While the journal thread is blocked in a segment write, a frame over the journal capacity is refused at once, before it takes a
    /// queued-append slot or a pending entry, and the stalled frame is written once the disk returns.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledJournalRefusesOversizedFrame(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, 1, cancellationToken);
        await AppendDurablyAsync(journal, "w", SmallPayload, cancellationToken);
        journal.Writer.Write.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("s"), new byte[SmallPayload], cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var oversized = journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("big"), new byte[MaxBytes + 1024L], cancellationToken).AsTask();
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(oversized.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var queued = journal.Journal.QueuedAppendsCounter.Value;
        var pendingCount = journal.Journal.PendingAppends.PendingCount;
        journal.Writer.Write.Release();
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        var failed = journal.Journal.HasFlushLoopFailure;
        await journal.ShutdownAsync();

        _ = await Assert.That(queued).IsEqualTo(1);
        _ = await Assert.That(pendingCount).IsEqualTo(1);
        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(journal.Recover(string.Empty, 0, cancellationToken)).IsEqualTo(Keys("s", "w"));
    }

    private static async Task AppendDurablyAsync(StallableJournal journal, string key, int payloadLength, CancellationToken cancellationToken)
    {
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default(key), new byte[payloadLength], cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    /// <summary>Appends fill frames until the one-megabyte segment holds room for one more fill frame, but not for two.</summary>
    /// <param name="journal">Journal whose current segment is filled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task FillSegmentAsync(StallableJournal journal, CancellationToken cancellationToken)
    {
        var fillFrame = FrameLength("fill", FillPayload);
        while (journal.Journal.ActiveSegmentWrittenBytes + (2L * fillFrame) <= MaxBytes)
            await AppendDurablyAsync(journal, "fill", FillPayload, cancellationToken);
    }

    private static int FrameLength(string key, int payloadLength)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = new byte[payloadLength],
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(record));
    }

    private static string Keys(params string[] keys)
    {
        var described = new string[keys.Length];
        for (var i = 0; i < keys.Length; i++)
            described[i] = CacheKey.Default(keys[i]).ToString();

        return StallableJournal.Describe(described);
    }

    /// <summary>
    /// Offers a frame larger than the room left in the filled segment (it needs a roll the segment count forbids), then a fill-sized frame
    /// that fits.
    /// </summary>
    /// <param name="journal">Journal whose only allowed segment is filled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>Whether the journal pipeline failed meanwhile.</returns>
    private static async Task<bool> RefuseRollThenFitAsync(StallableJournal journal, CancellationToken cancellationToken)
    {
        var refused = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(
            journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("roll"), new byte[(2 * FillPayload) + 1024], cancellationToken));
        _ = await Assert.That(refused.Message).Contains("segment count");
        await AppendDurablyAsync(journal, "fits", FillPayload, cancellationToken);
        return journal.Journal.HasFlushLoopFailure;
    }

    /// <summary>Writes a segment holding only its file header, as a roll target pre-created before a crash.</summary>
    /// <param name="path">Segment path.</param>
    private static void WriteHeaderOnlySegment(string path)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        RandomAccess.Write(handle, header, 0);
    }

    private Task<StallableJournal> CreateSingleSegmentJournalAsync(CancellationToken cancellationToken) => StallableJournal.CreateAsync(
        Dir,
        false,
        JournalSegmentLimits.DefaultMaxTotalBytesMb,
        1,
        1,
        cancellationToken);

    private long SegmentLength(int segmentIndex) => new FileInfo(JournalReadPath.BuildSegmentPath(Dir, segmentIndex)).Length;
}
