using System;
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

    private readonly long _maxSegmentBytes;

    internal JournalSegmentPolicy(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _maxSegmentBytes = ClampMb(options.JournalMaxSegmentMb, JournalSegmentLimits.DefaultMaxSegmentMb, JournalSegmentLimits.HardMaxSegmentMb);
        SegmentCountProbeLimit = Clamp(options.JournalMaxSegmentCount, JournalSegmentLimits.DefaultMaxSegmentCount, JournalSegmentLimits.HardMaxSegmentCount);
        MaxTotalBytes = ClampMb(options.JournalMaxTotalBytesMb, JournalSegmentLimits.DefaultMaxTotalBytesMb, JournalSegmentLimits.HardMaxTotalBytesMb);
        HighWaterBytes = MaxTotalBytes * JournalSegmentLimits.HighWaterPercent / 100L;
    }

    internal long HighWaterBytes { get; }

    internal long MaxTotalBytes { get; }

    private int SegmentCountProbeLimit { get; }

    internal static string EvaluatePressureState(long usedBytes, long highWaterBytes, long maxBytes) => (usedBytes >= maxBytes, usedBytes >= highWaterBytes) switch
    {
        (true, _) => "critical",
        (false, true) => "high",
        (false, false) => "normal",
    };

    /// <summary>
    /// Upper bound on the segments the journal thread adds, from a producer-side read up to and including the roll of an incoming frame, for
    /// the backlog admitted ahead of it and for its own roll (issue #703). The open of a missing current segment is not included.
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
        // incoming frame F rolls out of segment R. Leaving segment i took c_i + g_(i+1) >= W + 1, with g_(R+1) = F. Summing over
        // i = 1..R: R * (W + 1) <= sum(c_i) + sum(g_2..g_R) + F <= 2P + F, since each g_i is part of c_i. So R <= (2P + F) / (W + 1),
        // and F's own roll adds one more. A roll into a pre-created target that already holds frames adds no segment; the chain skips
        // that segment, and the bound still holds for the new ones. When the next target is pre-created with at most a header (counted,
        // and still starting at the header once rolled into), the chain covers every roll, and the first of the R + 1 rolls adds none.
        var usableSegmentBytes = _maxSegmentBytes - JournalFraming.FileHeaderSize;
        var backlogRolls = Math.Min(pendingCount, ((2L * pendingBytes) + incomingFrameBytes) / (usableSegmentBytes + 1L));
        return rollTargetCounted ? backlogRolls : backlogRolls + 1L;
    }

    /// <summary>
    /// Refuses an append before it enters the ring when the journal thread might reject it for capacity (issue #703): a plain append has
    /// no ack to carry that rejection back to its caller. Uses the journal thread's own comparisons, widened by upper bounds for the
    /// appends still ahead of this one: their bytes, one header each (a roll or the open of a missing or empty segment), and the
    /// segments they can add (see <see cref="BoundNewSegments" />). The caller supplies a consistent read and holds the mutation gate
    /// (see <c language="csharp">EnsureAppendAdmission</c>).
    /// </summary>
    /// <param name="state">Producer-side read of the journal capacity state.</param>
    /// <param name="incomingFrameBytes">Length of the frame to admit.</param>
    /// <exception cref="JournalCapacityExceededException">The frame never fits an empty segment, or it may exceed the total byte or segment count limit.</exception>
    internal void EnsureAdmissionOrThrow(in JournalAdmissionSnapshot state, int incomingFrameBytes)
    {
        // A frame that does not fit an empty segment would roll without end on the journal thread.
        if (ShouldRollSegment(JournalFraming.FileHeaderSize, incomingFrameBytes))
            throw new JournalCapacityExceededException(FrameExceedsSegmentMessage);

        var headerSlack = JournalFraming.FileHeaderSize * (state.PendingCount + 1L);
        if (state.TotalBytes + state.PendingBytes + incomingFrameBytes + headerSlack > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);

        // A roll only checks the segment count, so a frame that cannot need one is not bounded by it.
        if (!ShouldRollSegment(state.ActiveSegmentBytes + state.PendingBytes, incomingFrameBytes))
            return;

        var openedSegments = state.OpenCreatesSegment ? 1L : 0L;
        if (state.SegmentCount + openedSegments + BoundNewSegments(state.PendingBytes, state.PendingCount, incomingFrameBytes, state.RollTargetCounted) > SegmentCountProbeLimit)
            throw new JournalCapacityExceededException(SegmentCountExceededMessage);
    }

    internal void EnsureAppendCapacityOrThrow(long onDiskTotalBytes, int incomingFrameBytes)
    {
        var totalAfterAppend = onDiskTotalBytes + incomingFrameBytes;
        if (totalAfterAppend > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);
    }

    internal void EnsureRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) => EnsureCapacityOrThrow(onDiskSegmentCount + 1, onDiskTotalBytes);

    /// <summary>Roll capacity check for a pre-created target (crash aftermath): the target file is already counted, so it must not consume another segment slot.</summary>
    /// <param name="onDiskSegmentCount">Current on-disk journal segment count, including the pre-created target.</param>
    /// <param name="onDiskTotalBytes">Current on-disk journal total bytes, including the pre-created target header.</param>
    internal void EnsurePrecreatedRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) =>
        EnsureCapacityOrThrow(onDiskSegmentCount, onDiskTotalBytes);

    internal bool ShouldRollSegment(long activeSegmentWrittenBytes, int incomingFrameBytes) => activeSegmentWrittenBytes + incomingFrameBytes > _maxSegmentBytes;

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
