using System;
using System.Globalization;

namespace Squirix.Server.Storage.Journaling.Limits;

/// <summary>Enforces Pipelined segment count and total byte caps.</summary>
internal sealed class JournalSegmentPolicy
{
    private readonly long _maxSegmentBytes;
    private readonly long _maxTotalBytes;

    public JournalSegmentPolicy(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _maxSegmentBytes = ClampMb(options.JournalMaxSegmentMb, JournalSegmentLimits.DefaultMaxSegmentMb, JournalSegmentLimits.HardMaxSegmentMb);
        SegmentCountProbeLimit = Clamp(options.JournalMaxSegmentCount, JournalSegmentLimits.DefaultMaxSegmentCount, JournalSegmentLimits.HardMaxSegmentCount);
        _maxTotalBytes = ClampMb(options.JournalMaxTotalBytesMb, JournalSegmentLimits.DefaultMaxTotalBytesMb, JournalSegmentLimits.HardMaxTotalBytesMb);
    }

    internal int SegmentCountProbeLimit { get; }

    public void EnsureAppendCapacityOrThrow(long onDiskTotalBytes, int incomingFrameBytes)
    {
        var totalAfterAppend = onDiskTotalBytes + incomingFrameBytes;
        if (totalAfterAppend > _maxTotalBytes)
        {
            throw new JournalCapacityExceededException(
                $"journal total bytes {totalAfterAppend.ToString(CultureInfo.InvariantCulture)} exceed limit {_maxTotalBytes.ToString(CultureInfo.InvariantCulture)}.");
        }
    }

    public void EnsureRollCapacityOrThrow(int onDiskSegmentCount, long onDiskTotalBytes) => EnsureCapacityOrThrow(onDiskSegmentCount + 1, onDiskTotalBytes);

    public bool ShouldRollSegment(long activeSegmentWrittenBytes, int incomingFrameBytes) => activeSegmentWrittenBytes + incomingFrameBytes > _maxSegmentBytes;

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
        {
            throw new JournalCapacityExceededException(
                $"journal segment count {segmentCount.ToString(CultureInfo.InvariantCulture)} exceeds limit {SegmentCountProbeLimit.ToString(CultureInfo.InvariantCulture)}.");
        }

        if (totalBytes > _maxTotalBytes)
        {
            throw new JournalCapacityExceededException(
                $"journal total bytes {totalBytes.ToString(CultureInfo.InvariantCulture)} exceed limit {_maxTotalBytes.ToString(CultureInfo.InvariantCulture)}.");
        }
    }
}
