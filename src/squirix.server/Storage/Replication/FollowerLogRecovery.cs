using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Startup recovery: rebuilds the in-memory log from the durable frame file.</summary>
internal static class FollowerLogRecovery
{
    private const int FrameHeaderByteCount = 9;

    internal static async Task RecoverLogFileAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        // A published snapshot restores the committed baseline; the durable log then continues from its included
        // index plus one when the covered prefix was compacted away.
        var snapshotBase = await RestoreSnapshotBaseAsync(journal, owner, cancellationToken).ConfigureAwait(false);

        // Generation 0 means the group has never persisted a log, so nothing exists beyond the snapshot baseline.
        if (journal.LogGeneration == 0UL)
        {
            await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
            return;
        }

        var handle = File.OpenHandle(
            journal.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using (handle)
            await RecoverOpenedFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Empties the generation file the active one superseded, which a rewrite or a crash may have left holding a full copy of the log.</summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the superseded file is empty.</returns>
    internal static async Task EmptySupersededLogAsync(FollowerLogJournal journal, CancellationToken cancellationToken)
    {
        var path = journal.Paths.GetLogSlotPath(1 - journal.LogSlot);
        if (!File.Exists(path) || new FileInfo(path).Length == 0)
            return;

        await WorkPool.RunAsync(new RecoveryTruncateWork(path, 0), TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
    }

    private static async Task RecoverOpenedFileAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        if (await TryResetShortFileAsync(handle, journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false))
            return;

        journal.ClearEntries();

        // A populated log starts at index one (full history), right after the compacted snapshot
        // base, or — when a newer snapshot was published after the last compaction — inside the
        // snapshot-covered prefix. In the third shape the discarded head is backed by the published
        // snapshot, so the walk resumes from the last journaled index below the first frame. Trusting
        // the snapshot base against a full log would misread its first frame as a committed gap, so
        // the starting index is derived from the first frame.
        var firstIndex = await PeekFirstFrameIndexAsync(handle, cancellationToken).ConfigureAwait(false);
        if (await TryResetTornFirstFrameAsync(journal, owner, firstIndex, snapshotBase, cancellationToken).ConfigureAwait(false))
            return;

        var walkBase = DeriveWalkBaseIndex(firstIndex, snapshotBase);
        owner.LastLogIndex = walkBase;

        var result = await WalkFramesAsync(journal, owner, handle, cancellationToken).ConfigureAwait(false);
        await FinishRecoveredWalkAsync(handle, journal, owner, snapshotBase, walkBase, result, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> TryResetShortFileAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        // A header-only file carries no frames, so only the snapshot baseline (if any) is durable.
        if (RandomAccess.GetLength(handle) != GroupLogSlots.HeaderSize)
            return false;

        await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static async Task<bool> TryResetTornFirstFrameAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong firstIndex,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        if (firstIndex != 0 || snapshotBase == 0)
            return false;

        // The first suffix frame after the validated snapshot base is torn; the entire suffix is
        // unrecoverable. Truncate the file to the header and restart from the snapshot baseline,
        // but only while the snapshot covers the whole committed prefix: destroying the file below
        // a higher durable commit watermark would turn a repairable shortfall into a permanent one.
        if (snapshotBase < owner.Meta.CommitIndex)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw new InvalidDataException($"Replica group '{owner.GroupId}' first journal frame above snapshot base '{snapshotBase}' is unreadable while the commit watermark '{owner.Meta.CommitIndex}' exceeds it.");
        }

        await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static async Task FinishRecoveredWalkAsync(
        SafeFileHandle handle,
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        ulong walkBase,
        WalkResult result,
        CancellationToken cancellationToken)
    {
        // A crash between snapshot publication and journal truncation leaves the old journal in place; a
        // surviving frame at the restored snapshot's included index carrying a different term means the
        // suffix above it belongs to a divergent history and is discarded, restarting from the snapshot.
        if (result.BoundaryDivergent)
        {
            await TruncateAndResetAsync(journal, owner, GroupLogSlots.HeaderSize, snapshotBase, cancellationToken).ConfigureAwait(false);
            return;
        }

        // A torn trailing frame is truncated back to the last valid boundary on disk,
        // since a CRC mismatch alone cannot tell an appended tail from corruption.
        if (result.Truncated && result.LastValidEnd < RandomAccess.GetLength(handle))
            await ScheduleTruncateAsync(journal, result.LastValidEnd, cancellationToken).ConfigureAwait(false);

        await SnapshotBoundaryWatermarks.CompleteWalkAsync(journal, owner, snapshotBase, (walkBase, result.LastValidEnd), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Fails recovery for a gap within the committed region.</summary>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="nextLogIndex">The index of the missing frame.</param>
    /// <exception cref="InvalidDataException">The gap lies within the committed region.</exception>
    private static WalkResult CommittedGap(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
    {
        if (nextLogIndex > owner.Meta.CommitIndex)
            return new WalkResult(lastValidEnd, true);
        owner.Readiness = FollowerLogReadiness.Failed;
        throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log has a gap at index '{nextLogIndex}'.");
    }

    /// <summary>Derives the index the frame walk resumes from, given the first durable frame and the snapshot base.</summary>
    /// <remarks>
    ///     <para>
    ///     A populated log starts at index one (full history), right after the compacted snapshot base, or —
    ///     when a newer snapshot was published after the last compaction — inside the snapshot-covered prefix.
    ///     In the third shape the discarded head is backed by the published snapshot, so the walk resumes from
    ///     the last journaled index below the first frame.
    ///     </para>
    ///     <para>
    ///     Trusting the snapshot base against a full log would misread its first frame as a committed gap, so
    ///     the starting index is derived from the first frame instead.
    ///     </para>
    /// </remarks>
    /// <param name="firstIndex">The log index of the first durable frame, or zero when no valid frame opens the file.</param>
    /// <param name="snapshotBase">The restored snapshot baseline index.</param>
    /// <returns>The last journaled index below the first frame the walk continues from.</returns>
    private static ulong DeriveWalkBaseIndex(ulong firstIndex, ulong snapshotBase)
    {
        return true switch
        {
            _ when firstIndex == 1 || snapshotBase == 0 => 0UL,
            _ when firstIndex <= snapshotBase => firstIndex - 1UL,
            _ when firstIndex == snapshotBase + 1 => snapshotBase,
            _ => 0UL,
        };
    }

    /// <summary>Detects a surviving journal frame whose term diverges from the restored snapshot at its included index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="logIndex">The validated frame's log index.</param>
    /// <param name="term">The validated frame's term.</param>
    /// <returns>The divergent-boundary result, or <see langword="null" /> when the frame agrees with the snapshot.</returns>
    private static WalkResult? DivergentBoundary(FollowerLogJournal journal, ulong logIndex, ulong term)
    {
        var baseline = journal.SnapshotBaseline;
        return baseline.LastIncludedIndex != logIndex || baseline.LastIncludedTerm == term ? null : new WalkResult(GroupLogSlots.HeaderSize, true, true);
    }

    /// <summary>Reads the log index of the first frame from the fixed header boundary.</summary>
    /// <param name="handle">The open log file handle.</param>
    /// <param name="cancellationToken">A token to observe while reading.</param>
    /// <returns>The first frame's log index, or <c language="csharp">0</c> when no valid frame opens the file.</returns>
    private static async Task<ulong> PeekFirstFrameIndexAsync(SafeFileHandle handle, CancellationToken cancellationToken)
    {
        const int start = GroupLogSlots.HeaderSize;
        var length = RandomAccess.GetLength(handle);
        if (length - start < FrameHeaderByteCount)
            return 0UL;

        var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
        try
        {
            var headerEnd = await HandleEx.ReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), start, cancellationToken).ConfigureAwait(false);
            if (headerEnd == null || !GroupLogCodec.TryReadFrameHeaderLength(frameHeader.AsSpan(0, FrameHeaderByteCount), out var frameLength))
                return 0UL;

            if (frameLength > length - start)
                return 0UL;

            var frame = ArrayPool<byte>.Shared.Rent(frameLength);
            try
            {
                frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
                var frameEnd = await HandleEx.ReadExactAsync(
                    handle,
                    frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount),
                    start + FrameHeaderByteCount,
                    cancellationToken).ConfigureAwait(false);
                return frameEnd != null && GroupLogCodec.TryReadFrameFields(frame.AsSpan(0, frameLength), out var logIndex, out _) ? logIndex : 0UL;
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(frame);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frameHeader);
        }
    }

    /// <summary>
    /// Reads the frame starting at <paramref name="frameStart" /> into scratch buffers: validates the
    /// header, grows the frame buffer when needed, and reads payload bytes at their explicit offset.
    /// A partial header/payload or an oversized declaration yields a terminal torn-tail result.
    /// </summary>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="handle">The open log file handle.</param>
    /// <param name="frameHeader">Scratch buffer for the fixed-size frame header.</param>
    /// <param name="previousFrame">The previously rented frame buffer to reuse when large enough.</param>
    /// <param name="frameStart">File offset of the frame to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The read outcome: buffers, frame length, next log index, and a terminal result when torn.</returns>
    private static async Task<FrameReadOutcome> ReadNextFrameAsync(
        IFollowerLogContext owner,
        SafeFileHandle handle,
        byte[] frameHeader,
        byte[]? previousFrame,
        long frameStart,
        CancellationToken cancellationToken)
    {
        var nextLogIndex = owner.LastLogIndex + 1;

        // A partial header is a torn trailing frame.
        if (RandomAccess.GetLength(handle) - frameStart < FrameHeaderByteCount)
            return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

        var headerEnd = await HandleEx.ReadExactAsync(handle, frameHeader.AsMemory(0, FrameHeaderByteCount), frameStart, cancellationToken).ConfigureAwait(false);
        if (headerEnd == null)
            return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

        // A header that fails structural validation is either a torn tail or a corrupt committed frame.
        if (!GroupLogCodec.TryReadFrameHeaderLength(frameHeader.AsSpan(0, FrameHeaderByteCount), out var frameLength))
            return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

        // A frame that ends past EOF is a torn trailing frame.
        if (RandomAccess.GetLength(handle) - (frameStart + FrameHeaderByteCount) < frameLength - FrameHeaderByteCount)
            return new FrameReadOutcome(previousFrame ?? ArrayPool<byte>.Shared.Rent(1), null, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex));

        var previous = previousFrame;
        var frame = RentFrameBuffer(previous, frameLength, out var exhausted);

        var payloadOffset = frameStart + FrameHeaderByteCount;
        frameHeader.AsSpan(0, FrameHeaderByteCount).CopyTo(frame);
        var payloadEnd = await HandleEx.ReadExactAsync(handle, frame.AsMemory(FrameHeaderByteCount, frameLength - FrameHeaderByteCount), payloadOffset, cancellationToken)
                                       .ConfigureAwait(false);

        // The frame content is incomplete: hand ownership back to the caller's pooling path.
        return payloadEnd == null
            ? new FrameReadOutcome(frame, exhausted, 0, nextLogIndex, UncommittedTail(owner, frameStart, nextLogIndex))
            : new FrameReadOutcome(frame, exhausted, frameLength, nextLogIndex, null);
    }

    /// <summary>
    /// Reconciles the published snapshot's term, vote, topology fingerprint, configuration generation, and
    /// watermarks with the durable metadata, persisting the repaired metadata before readiness so a replica
    /// restarted after a crash between snapshot publication and metadata advance does not load stale state.
    /// </summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="snapshot">The published snapshot loaded from the disk.</param>
    /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
    private static async Task ReconcileSnapshotMetadataAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        var included = snapshot.LastIncludedIndex;
        var commit = Math.Max(owner.Meta.CommitIndex, Math.Min(snapshot.CommitIndex, included));
        var candidate = owner.Meta;
        var changed = false;

        if (commit > candidate.CommitIndex)
        {
            candidate = candidate with { CommitIndex = commit };
            changed = true;
        }

        // A snapshot is authoritative for the covered prefix: a higher included term advances the durable
        // term and clears any vote cast in the older term before readiness, mirroring InstallAsync.
        var termRaised = snapshot.LastIncludedTerm > candidate.CurrentTerm;
        if (termRaised)
        {
            candidate = candidate with { CurrentTerm = snapshot.LastIncludedTerm, VotedFor = string.Empty };
            changed = true;
        }

        if (candidate.TopologyFingerprint.IsEmpty && !snapshot.TopologyFingerprint.IsEmpty)
        {
            candidate = candidate with { TopologyFingerprint = snapshot.TopologyFingerprint };
            changed = true;
        }

        if (snapshot.ConfigurationGeneration > candidate.ConfigurationGeneration)
        {
            candidate = candidate with { ConfigurationGeneration = snapshot.ConfigurationGeneration };
            changed = true;
        }

        if (!changed)
            return;

        // A raised term is mirrored into both slots, like every term step; other repairs go to one slot.
        var persist = termRaised
            ? FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken)
            : FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken);
        await persist.ConfigureAwait(false);
        owner.Meta = candidate;
    }

    /// <summary>Records a CRC-validated frame, keeping the payload only above the applied watermark.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="nextLogIndex">The index the frame must carry.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="frame">The complete frame at its start offset.</param>
    /// <returns>The recovery result when the frame must be truncated; otherwise <see langword="null" />.</returns>
    private static WalkResult? RecordFrame(FollowerLogJournal journal, IFollowerLogContext owner, ulong nextLogIndex, long lastValidEnd, ReadOnlySpan<byte> frame)
    {
        // Frames at or below the applied watermark need only their index and term, not their payload.
        if (nextLogIndex <= owner.Meta.LastAppliedIndex)
        {
            if (!GroupLogCodec.TryReadFrameFields(frame, out var logIndex, out var term))
                return UncommittedTail(owner, lastValidEnd, nextLogIndex);

            if (logIndex != nextLogIndex)
                return CommittedGap(owner, lastValidEnd, nextLogIndex);

            if (DivergentBoundary(journal, logIndex, term) is { } divergent)
                return divergent;

            journal.AddEntryOffset(logIndex, lastValidEnd, term);
            owner.LastLogIndex = logIndex;
            return null;
        }

        if (!GroupLogCodec.TryReadFrame(frame, out var entry))
            return UncommittedTail(owner, lastValidEnd, nextLogIndex);

        if (entry.LogIndex != nextLogIndex)
            return CommittedGap(owner, lastValidEnd, nextLogIndex);

        if (DivergentBoundary(journal, entry.LogIndex, entry.Term) is { } divergentBoundary)
            return divergentBoundary;

        journal.AddEntry(in entry, lastValidEnd, entry.Term);
        owner.LastLogIndex = entry.LogIndex;
        return null;
    }

    /// <summary>Rents a frame buffer of at least <paramref name="frameLength" /> bytes, reusing the current one when it fits.</summary>
    /// <param name="current">The previously rented buffer, if any.</param>
    /// <param name="frameLength">The required frame length.</param>
    /// <param name="exhausted">The returned buffer when the current one was replaced; the caller must return it to the pool.</param>
    /// <returns>A rented buffer large enough for the frame.</returns>
    private static byte[] RentFrameBuffer(byte[]? current, int frameLength, out byte[]? exhausted)
    {
        // Detach before returning so a Rent failure can never leave an already-returned
        // array referenced here for a second return in the outer finally.
        exhausted = null;
        if (current != null && current.Length >= frameLength)
            return current;

        exhausted = current;
        return ArrayPool<byte>.Shared.Rent(frameLength);
    }

    /// <summary>Restores the idempotency baseline from a compatible published snapshot and returns its included index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="cancellationToken">A token to observe while reading.</param>
    /// <returns>The snapshot's included index, or <c language="csharp">0</c> when no snapshot is published.</returns>
    /// <exception cref="InvalidDataException">
    /// The published snapshot is corrupt, belongs to another group, or conflicts
    /// with the durable topology metadata.
    /// </exception>
    private static async Task<ulong> RestoreSnapshotBaseAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        if (!journal.Snapshot.SnapshotExists)
            return 0UL;

        try
        {
            var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
            if (snapshot == null)
                return 0UL;

            if (!string.Equals(snapshot.Value.GroupId, owner.GroupId, StringComparison.Ordinal))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot belongs to group '{snapshot.Value.GroupId}'.");
            }

            if (FollowerLogSnapshot.SnapshotTopologyMismatch(owner, snapshot.Value) != null)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot topology or configuration generation conflicts with durable metadata.");
            }

            // A snapshot whose commit index falls below its included index is malformed: the committed prefix it
            // claims to cover is internally inconsistent, and adopting its boundary would let LastAppliedIndex
            // exceed CommitIndex. ReconcileSnapshotMetadataAsync assumes the decoder already rejected this; guard
            // here so recovery fails readiness before persisting an incoherent watermark, matching
            // ValidateInstallEligibility on the installation path.
            if (snapshot.Value.CommitIndex < snapshot.Value.LastIncludedIndex)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException(
                    $"Replica group '{owner.GroupId}' snapshot commit index '{snapshot.Value.CommitIndex}' is below its included index '{snapshot.Value.LastIncludedIndex}'.");
            }

            // Terms start at 1. A zero-included term collides with the unverifiable-term sentinel used by
            // TermAtApplied and would make DivergentBoundary discard the entire durable suffix.
            if (snapshot.Value is { LastIncludedIndex: > 0UL, LastIncludedTerm: 0UL })
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot included term is zero at index '{snapshot.Value.LastIncludedIndex}'.");
            }

            // Reconcile and durably persist the snapshot metadata BEFORE mutating in-memory recovery state. If the
            // persisting is canceled (OperationCanceledException, preserved as retryable by PersistMetaOrFailReadinessAsync)
            // or fails, the instance is left without partially restored idempotency/baseline, so a retry of OpenAsync on
            // the same instance cannot operate on inconsistent state. See F37.
            await ReconcileSnapshotMetadataAsync(journal, owner, snapshot.Value, cancellationToken).ConfigureAwait(false);
            owner.Idempotency.RestoreFromSnapshot(snapshot.Value.CommittedOutcomes, snapshot.Value.CapturedUtc, []);

            // Recovery rebuilds both indexes from the durable log after this point; the baseline is
            // restored without pruning because the walk owns the index lifecycle.
            owner.RestoreBaseline(new SnapshotBaseline(snapshot.Value.LastIncludedIndex, snapshot.Value.LastIncludedTerm));
            return snapshot.Value.LastIncludedIndex;
        }
        catch (InvalidDataException)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    private static Task ScheduleTruncateAsync(FollowerLogJournal journal, long truncateLength, CancellationToken cancellationToken)
    {
        var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
        return WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken);
    }

    private static async Task TruncateAndResetAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        long truncateLength,
        ulong snapshotBase,
        CancellationToken cancellationToken)
    {
        var work = new RecoveryTruncateWork(journal.LogPath, truncateLength);
        await WorkPool.RunAsync(work, TaskCreationOptions.None, cancellationToken).ConfigureAwait(false);
        journal.ClearEntries();

        // The truncation dropped every frame at or below the snapshot base, so nothing can apply them any more: the applied watermark
        // moves to the base, as a snapshot installation moves it. Keeping it below would point the applier at entries no frame holds,
        // and its catch-up would refuse the gap forever. The commit index already reaches the base, which the snapshot reconciliation
        // raised it to, so the watermark never passes the commit index.
        await SnapshotBoundaryWatermarks.ResetAndEnsureAsync(journal, owner, snapshotBase, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Fails recovery for an invalid frame within the committed region.</summary>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="nextLogIndex">The index of the invalid frame.</param>
    /// <exception cref="InvalidDataException">The invalid frame lies within the committed region.</exception>
    private static WalkResult UncommittedTail(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
    {
        if (nextLogIndex > owner.Meta.CommitIndex)
            return new WalkResult(lastValidEnd, true);
        owner.Readiness = FollowerLogReadiness.Failed;
        throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
    }

    private static async Task<WalkResult> WalkFramesAsync(FollowerLogJournal journal, IFollowerLogContext owner, SafeFileHandle handle, CancellationToken cancellationToken)
    {
        long lastValidEnd = GroupLogSlots.HeaderSize;

        // Two pool rents for the whole walk instead of two per frame; the frame buffer grows on demand.
        var frameHeader = ArrayPool<byte>.Shared.Rent(FrameHeaderByteCount);
        byte[]? frame = null;
        try
        {
            while (lastValidEnd < RandomAccess.GetLength(handle))
            {
                var outcome = await ReadNextFrameAsync(owner, handle, frameHeader, frame, lastValidEnd, cancellationToken).ConfigureAwait(false);
                if (outcome.Terminal != null)
                    return outcome.Terminal.Value;

                frame = outcome.Frame;
                if (outcome.Exhausted != null)
                    ArrayPool<byte>.Shared.ReturnCleared(outcome.Exhausted);

                var tail = RecordFrame(journal, owner, outcome.NextLogIndex, lastValidEnd, outcome.Frame.AsSpan(0, outcome.FrameLength));
                if (tail != null)
                    return tail.Value;

                lastValidEnd += outcome.FrameLength;
            }

            return new WalkResult(lastValidEnd, false);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frameHeader);
            if (frame != null)
                ArrayPool<byte>.Shared.ReturnCleared(frame);
        }
    }

    /// <summary>Outcome of reading a single recovery frame into scratch buffers.</summary>
    /// <param name="Frame">The rented frame buffer holding header plus payload.</param>
    /// <param name="Exhausted">The previously rented buffer to return to the pool, when it was replaced.</param>
    /// <param name="FrameLength">Total length of the frame read; zero when torn.</param>
    /// <param name="NextLogIndex">The log index expected for this frame position.</param>
    /// <param name="Terminal">A torn-tail walk result; non-null when the walk must stop.</param>
    [Immutable]
    private readonly record struct FrameReadOutcome(byte[] Frame, byte[]? Exhausted, int FrameLength, ulong NextLogIndex, WalkResult? Terminal);

    /// <summary>Result of walking the log frames during startup recovery.</summary>
    /// <param name="LastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="Truncated">Determines whether a divergent tail was truncated.</param>
    /// <param name="BoundaryDivergent">
    /// Determines whether the surviving journal diverges from the restored
    /// snapshot at the snapshot's included index.
    /// </param>
    [Immutable]
    private readonly record struct WalkResult(long LastValidEnd, bool Truncated, bool BoundaryDivergent = false);

    /// <summary>Background work that durably truncates the log during startup recovery.</summary>
    [Immutable]
    private sealed class RecoveryTruncateWork : IWorkPoolItem
    {
        private readonly long _length;
        private readonly string _path;

        internal RecoveryTruncateWork(string path, long length)
        {
            _path = path;
            _length = length;
        }

        void IWorkPoolItem.Execute()
        {
            using var handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, FileOptions.None);
            RandomAccess.SetLength(handle, _length);
            RandomAccess.FlushToDisk(handle);
        }
    }
}
