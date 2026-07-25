using System;
using Squirix.Server.Errors;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Enforces Pipelined segment count and total byte caps.</summary>
internal sealed class JournalSegmentPolicy
{
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

    internal static string EvaluatePressureState(long usedBytes, long highWaterBytes, long maxBytes)
    {
        if (usedBytes >= maxBytes)
            return "critical";

        if (usedBytes >= highWaterBytes)
            return "high";

        return "normal";
    }

    internal void EnsureAppendCapacityOrThrow(long onDiskTotalBytes, int incomingFrameBytes)
    {
        var totalAfterAppend = onDiskTotalBytes + incomingFrameBytes;
        if (totalAfterAppend > MaxTotalBytes)
            throw new JournalCapacityExceededException(TotalBytesExceededMessage);
    }

    internal void EnsureRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) => EnsureCapacityOrThrow(onDiskSegmentCount + 1, onDiskTotalBytes);

    internal bool ShouldRollSegment(long activeSegmentWrittenBytes, int incomingFrameBytes) => activeSegmentWrittenBytes + incomingFrameBytes > _maxSegmentBytes;

    private static int Clamp(int value, int defaultValue, int hardMax)
    {
        if (value <= 0)
            return defaultValue;

        return Math.Min(value, hardMax);
    }

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
