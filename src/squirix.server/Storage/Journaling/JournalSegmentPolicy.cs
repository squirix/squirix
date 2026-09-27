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
    /// Refuses an append before it enters the ring when the journal thread might reject it for capacity (issue #703): a plain append has
    /// no ack to carry that rejection back to its caller. Uses the journal thread's own comparisons, widened by upper bounds for the
    /// appends still ahead of this one: their bytes, one header each (a roll or the open of a missing or empty segment), and one new
    /// segment each. The caller supplies a consistent read and holds the mutation gate (see <c language="csharp">EnsureAppendAdmission</c>).
    /// </summary>
    /// <param name="onDiskTotalBytes">Journal total bytes, read after the pending counters.</param>
    /// <param name="pendingBytes">Summed frame length of the appends admitted and not yet written.</param>
    /// <param name="pendingCount">Number of the appends admitted and not yet written.</param>
    /// <param name="activeSegmentWrittenBytes">Active segment length, read after the pending counters.</param>
    /// <param name="onDiskSegmentCount">Journal segment count, read after the pending counters.</param>
    /// <param name="incomingFrameBytes">Length of the frame to admit.</param>
    /// <exception cref="JournalCapacityExceededException">The frame never fits an empty segment, or it may exceed the total byte or segment count limit.</exception>
    internal void EnsureAdmissionOrThrow(long onDiskTotalBytes, long pendingBytes, int pendingCount, long activeSegmentWrittenBytes, int onDiskSegmentCount, int incomingFrameBytes)
    {
        // A frame that does not fit an empty segment would roll without end on the journal thread.
        if (ShouldRollSegment(JournalFraming.FileHeaderSize, incomingFrameBytes))
            throw new JournalCapacityExceededException(FrameExceedsSegmentMessage);

        var headerSlack = JournalFraming.FileHeaderSize * (pendingCount + 1L);
        if (onDiskTotalBytes + pendingBytes + incomingFrameBytes + headerSlack > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);

        // A roll only checks the segment count, so a frame that cannot need one is not bounded by it.
        if (ShouldRollSegment(activeSegmentWrittenBytes + pendingBytes, incomingFrameBytes) && onDiskSegmentCount + pendingCount + 1L > SegmentCountProbeLimit)
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
