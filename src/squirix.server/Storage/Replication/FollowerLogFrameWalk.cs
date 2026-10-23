using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Startup recovery frame walk: reads the durable frames in log order and rebuilds the in-memory indexes from them.</summary>
internal static class FollowerLogFrameWalk
{
    private const int FrameHeaderByteCount = 9;

    /// <summary>Reads the log index of the first frame from the fixed header boundary.</summary>
    /// <param name="handle">The open log file handle.</param>
    /// <param name="cancellationToken">A token to observe while reading.</param>
    /// <returns>The first frame's log index, or <c language="csharp">0</c> when no valid frame opens the file.</returns>
    internal static async Task<ulong> PeekFirstFrameIndexAsync(SafeFileHandle handle, CancellationToken cancellationToken)
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

    /// <summary>Walks the frames after the file header, recording each CRC-validated frame in the in-memory indexes.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="handle">The open log file handle.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The walk result: the byte offset after the last valid frame, and whether the tail or the snapshot boundary must be truncated.</returns>
    internal static async Task<FollowerLogWalkResult> WalkFramesAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        SafeFileHandle handle,
        CancellationToken cancellationToken)
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

            return new FollowerLogWalkResult(lastValidEnd, false);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frameHeader);
            if (frame != null)
                ArrayPool<byte>.Shared.ReturnCleared(frame);
        }
    }

    /// <summary>Fails recovery for a gap within the committed region.</summary>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="nextLogIndex">The index of the missing frame.</param>
    /// <exception cref="InvalidDataException">The gap lies within the committed region.</exception>
    private static FollowerLogWalkResult CommittedGap(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
    {
        if (nextLogIndex > owner.Meta.CommitIndex)
            return new FollowerLogWalkResult(lastValidEnd, true);
        owner.Readiness = FollowerLogReadiness.Failed;
        throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log has a gap at index '{nextLogIndex}'.");
    }

    /// <summary>Detects a surviving journal frame whose term diverges from the restored snapshot at its included index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="logIndex">The validated frame's log index.</param>
    /// <param name="term">The validated frame's term.</param>
    /// <returns>The divergent-boundary result, or <see langword="null" /> when the frame agrees with the snapshot.</returns>
    private static FollowerLogWalkResult? DivergentBoundary(FollowerLogJournal journal, ulong logIndex, ulong term)
    {
        var baseline = journal.SnapshotBaseline;
        return baseline.LastIncludedIndex != logIndex || baseline.LastIncludedTerm == term ? null : new FollowerLogWalkResult(GroupLogSlots.HeaderSize, true, true);
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

    /// <summary>Records a CRC-validated frame, keeping the payload only above the applied watermark.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="nextLogIndex">The index the frame must carry.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="frame">The complete frame at its start offset.</param>
    /// <returns>The recovery result when the frame must be truncated; otherwise <see langword="null" />.</returns>
    private static FollowerLogWalkResult? RecordFrame(FollowerLogJournal journal, IFollowerLogContext owner, ulong nextLogIndex, long lastValidEnd, ReadOnlySpan<byte> frame)
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

    /// <summary>Fails recovery for an invalid frame within the committed region.</summary>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="lastValidEnd">The byte offset after the last valid frame.</param>
    /// <param name="nextLogIndex">The index of the invalid frame.</param>
    /// <exception cref="InvalidDataException">The invalid frame lies within the committed region.</exception>
    private static FollowerLogWalkResult UncommittedTail(IFollowerLogContext owner, long lastValidEnd, ulong nextLogIndex)
    {
        if (nextLogIndex > owner.Meta.CommitIndex)
            return new FollowerLogWalkResult(lastValidEnd, true);
        owner.Readiness = FollowerLogReadiness.Failed;
        throw new InvalidDataException($"Replica group '{owner.GroupId}' committed log frame at index '{nextLogIndex}' is corrupt.");
    }

    /// <summary>Outcome of reading a single recovery frame into scratch buffers.</summary>
    /// <param name="Frame">The rented frame buffer holding header plus payload.</param>
    /// <param name="Exhausted">The previously rented buffer to return to the pool, when it was replaced.</param>
    /// <param name="FrameLength">Total length of the frame read; zero when torn.</param>
    /// <param name="NextLogIndex">The log index expected for this frame position.</param>
    /// <param name="Terminal">A torn-tail walk result; non-null when the walk must stop.</param>
    [Immutable]
    private readonly record struct FrameReadOutcome(byte[] Frame, byte[]? Exhausted, int FrameLength, ulong NextLogIndex, FollowerLogWalkResult? Terminal);
}
