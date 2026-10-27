using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Enforces Pipelined segment count and total byte caps.</summary>
[Immutable]
internal sealed class JournalSegmentPolicy
{
    private const string FrameExceedsSegmentMessage = "journal frame exceeds the configured segment size.";
    private const string SegmentCountExceededMessage = "journal segment count exceeds configured limit.";
    private const string TotalBytesExceededMessage = "journal total bytes exceed configured limit.";

    private const int MinHighWaterSegmentCount = 2;

    private static readonly long LargestFrameBytes = Convert.ToInt64(JournalSegmentLimits.MinSegmentMb) * 1024L * 1024L;

    internal JournalSegmentPolicy(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        MaxSegmentBytes = ClampMb(options.JournalMaxSegmentMb, JournalSegmentLimits.DefaultMaxSegmentMb, JournalSegmentLimits.HardMaxSegmentMb);
        SegmentCountProbeLimit = Clamp(options.JournalMaxSegmentCount, JournalSegmentLimits.DefaultMaxSegmentCount, JournalSegmentLimits.HardMaxSegmentCount);
        MaxTotalBytes = ClampMb(options.JournalMaxTotalBytesMb, JournalSegmentLimits.DefaultMaxTotalBytesMb, JournalSegmentLimits.HardMaxTotalBytesMb);
        HighWaterBytes = MaxTotalBytes * JournalSegmentLimits.HighWaterPercent / 100L;
        HighWaterSegmentCount = Math.Max(MinHighWaterSegmentCount, Convert.ToInt32(Convert.ToInt64(SegmentCountProbeLimit) * JournalSegmentLimits.HighWaterPercent / 100L));
    }

    internal long HighWaterBytes { get; }

    /// <summary>Gets the configured segment size in bytes.</summary>
    internal long MaxSegmentBytes { get; }

    /// <summary>Gets the configured journal segment count cap.</summary>
    internal int MaxSegmentCount => SegmentCountProbeLimit;

    /// <summary>
    /// Gets a value indicating whether a full segment plus the largest journal frame stays below the high-water mark in bytes, and the high-water
    /// segment count stays below the segment count cap.
    /// </summary>
    /// <remarks>
    /// When it does not, a snapshot at the high-water mark may free no segment before the active one fills the quota, and a state transfer can wait for
    /// space that never comes.
    /// </remarks>
    internal bool LeavesHeadroomBelowHighWater => MaxSegmentBytes + LargestFrameBytes < HighWaterBytes && HighWaterSegmentCount < SegmentCountProbeLimit;

    internal int HighWaterSegmentCount { get; }

    internal long MaxTotalBytes { get; }

    private int SegmentCountProbeLimit { get; }

    internal static string EvaluatePressureState(long usedBytes, long highWaterBytes, long maxBytes) => (usedBytes >= maxBytes, usedBytes >= highWaterBytes) switch
    {
        (true, _) => "critical",
        (false, true) => "high",
        (false, false) => "normal",
    };

    /// <summary>Tells whether the journal holds at least the soft high-water share of its total byte cap or of its segment count cap.</summary>
    /// <param name="totalBytes">Current on-disk journal total bytes.</param>
    /// <param name="segmentCount">Current on-disk journal segment count.</param>
    /// <returns><see langword="true" /> when either the bytes or the segment count reached its high-water mark.</returns>
    internal bool IsAtHighWater(long totalBytes, int segmentCount) => totalBytes >= HighWaterBytes || segmentCount >= HighWaterSegmentCount;

    /// <summary>
    /// Upper bound on the segments the journal thread adds, from a producer-side read up to and including the roll of an incoming frame, for
    /// the backlog admitted ahead of it and for its own roll. The open of a missing current segment is not included.
    /// </summary>
    /// <param name="pendingBytes">Summed frame length of the appends admitted and not yet written.</param>
    /// <param name="pendingCount">Number of the appends admitted and not yet written.</param>
    /// <param name="incomingFrameBytes">Length of the incoming frame, assumed to roll.</param>
    /// <param name="rollTargetCounted">Whether the next roll target is already on disk, counted, and at most a file header.</param>
    /// <returns>The most segments that can be added before the incoming frame's roll is checked, plus one for that roll.</returns>
    internal long BoundNewSegments(long pendingBytes, int pendingCount, int incomingFrameBytes, bool rollTargetCounted)
    {
        // The journal thread places frames in order (next fit): it rolls when active + staged + frame exceeds the segment size S, and a
        // new segment starts at FileHeaderSize, so W = S - FileHeaderSize bytes of frames fit into it. Every admitted frame fits an empty
        // segment, so a frame adds at most one new segment: the backlog adds at most pendingCount. By bytes: say the backlog (P bytes)
        // rolls into new segments 1..R, segment i holds c_i backlog bytes and begins with the frame g_i that rolled into it, and the
        // incoming frame F rolls out of segment R. Leaving segment i took at least W + 1 bytes (c_i plus g_(i+1), where g_(R+1) is F).
        // Summed over every i from 1 to R, R times (W + 1) is at most sum(c_i) plus sum(g_2..g_R) plus F, which is at most 2P + F
        // because each g_i is part of c_i. Hence R is at most (2P + F) / (W + 1), and F's own roll adds one more. A roll into a pre-created target that already holds frames adds no segment; the chain skips
        // that segment, and the bound still holds for the new ones. When the next target is pre-created with at most a header (counted,
        // and still starting at the header once rolled into), the chain covers every roll, and the first of the R + 1 rolls adds none.
        var usableSegmentBytes = MaxSegmentBytes - JournalFraming.FileHeaderSize;
        var backlogRolls = Math.Min(pendingCount, ((2L * pendingBytes) + incomingFrameBytes) / (usableSegmentBytes + 1L));
        return rollTargetCounted ? backlogRolls : backlogRolls + 1L;
    }

    /// <summary>
    /// Decides whether an append may enter the ring, without throwing: a plain append has no ack to carry a capacity rejection back to its
    /// caller, so the journal thread might reject it. Uses the journal thread's own comparisons, widened by upper bounds for the
    /// appends still ahead of this one: their bytes, one header each (a roll or the open of a missing or empty segment), and the
    /// segments they can add (see <see cref="BoundNewSegments" />). The caller supplies a consistent read and holds the mutation gate
    /// (see <c language="csharp">EnsureAppendAdmission</c>).
    /// </summary>
    /// <param name="state">Producer-side read of the journal capacity state.</param>
    /// <param name="incomingFrameBytes">Length of the frame to admit.</param>
    /// <param name="refusal">The message naming the limit the frame may exceed, or <see langword="null" /> when it is admitted.</param>
    /// <returns><see langword="true" /> when the frame is admitted; <see langword="false" /> when the frame never fits an empty segment, or it may exceed the total byte or segment count limit.</returns>
    internal bool Admits(in JournalAdmissionSnapshot state, int incomingFrameBytes, [NotNullWhen(false)] out string? refusal)
    {
        // A frame that does not fit an empty segment would roll without end on the journal thread.
        if (ShouldRollSegment(JournalFraming.FileHeaderSize, incomingFrameBytes))
        {
            refusal = FrameExceedsSegmentMessage;
            return false;
        }

        var headerSlack = JournalFraming.FileHeaderSize * (state.PendingCount + 1L);
        if (state.TotalBytes + state.PendingBytes + incomingFrameBytes + headerSlack > MaxTotalBytes)
        {
            refusal = TotalBytesExceededMessage;
            return false;
        }

        // A roll only checks the segment count, so a frame that cannot need one is not bounded by it.
        if (ShouldRollSegment(state.ActiveSegmentBytes + state.PendingBytes, incomingFrameBytes))
        {
            var openedSegments = state.OpenCreatesSegment ? 1L : 0L;
            if (state.SegmentCount + openedSegments + BoundNewSegments(state.PendingBytes, state.PendingCount, incomingFrameBytes, state.RollTargetCounted) > SegmentCountProbeLimit)
            {
                refusal = SegmentCountExceededMessage;
                return false;
            }
        }

        refusal = null;
        return true;
    }

    /// <summary>Refuses an append before it enters the ring when <see cref="Admits" /> declines it.</summary>
    /// <param name="state">Producer-side read of the journal capacity state.</param>
    /// <param name="incomingFrameBytes">Length of the frame to admit.</param>
    /// <exception cref="JournalCapacityExceededException">The frame never fits an empty segment, or it may exceed the total byte or segment count limit.</exception>
    internal void EnsureAdmissionOrThrow(in JournalAdmissionSnapshot state, int incomingFrameBytes)
    {
        if (!Admits(in state, incomingFrameBytes, out var refusal))
            throw new JournalCapacityExceededException(refusal);
    }

    internal void EnsureAppendCapacityOrThrow(long onDiskTotalBytes, int incomingFrameBytes)
    {
        var totalAfterAppend = onDiskTotalBytes + incomingFrameBytes;
        if (totalAfterAppend > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);
    }

    /// <summary>
    /// Refuses a frame that does not fit even an empty segment (a file header plus the frame exceed the segment size): no roll can ever
    /// make it fit. Shared by append admission and the journal thread.
    /// </summary>
    /// <param name="incomingFrameBytes">Length of the frame to place.</param>
    /// <exception cref="JournalCapacityExceededException">The frame never fits an empty segment.</exception>
    internal void EnsureFitsEmptySegmentOrThrow(int incomingFrameBytes)
    {
        if (ShouldRollSegment(JournalFraming.FileHeaderSize, incomingFrameBytes))
            throw new JournalCapacityExceededException(FrameExceedsSegmentMessage);
    }

    internal void EnsureRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) => EnsureCapacityOrThrow(onDiskSegmentCount + 1, onDiskTotalBytes);

    /// <summary>Roll capacity check for a pre-created target (crash aftermath): the target file is already counted, so it must not consume another segment slot.</summary>
    /// <param name="onDiskSegmentCount">Current on-disk journal segment count, including the pre-created target.</param>
    /// <param name="onDiskTotalBytes">Current on-disk journal total bytes, including the pre-created target header.</param>
    internal void EnsurePrecreatedRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) =>
        EnsureCapacityOrThrow(onDiskSegmentCount, onDiskTotalBytes);

    internal bool ShouldRollSegment(long activeSegmentWrittenBytes, int incomingFrameBytes) => activeSegmentWrittenBytes + incomingFrameBytes > MaxSegmentBytes;

    private static int Clamp(int value, int defaultValue, int hardMax) => value <= 0 ? defaultValue : Math.Min(value, hardMax);

    private static long ClampMb(int valueMb, int defaultMb, int hardMaxMb)
    {
        var mb = valueMb <= 0 ? defaultMb : Math.Min(valueMb, hardMaxMb);
        return Convert.ToInt64(mb) * 1024L * 1024L;
    }

    private void EnsureCapacityOrThrow(int segmentCount, long totalBytes)
    {
        if (segmentCount > SegmentCountProbeLimit)
            throw new JournalCapacityExceededException(SegmentCountExceededMessage);

        if (totalBytes > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);
    }
}
