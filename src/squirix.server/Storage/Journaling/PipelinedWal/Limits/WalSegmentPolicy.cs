using System;
using System.Globalization;
using Squirix.Server.Storage;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Limits;

/// <summary>Enforces PipelinedWal segment count and total byte caps.</summary>
internal sealed class WalSegmentPolicy
{
    private readonly long _maxSegmentBytes;
    private readonly int _maxSegmentCount;
    private readonly long _maxTotalBytes;

    public WalSegmentPolicy(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _maxSegmentBytes = ClampMb(options.JournalMaxSegmentMb, WalSegmentLimits.DefaultMaxSegmentMb, WalSegmentLimits.HardMaxSegmentMb);
        _maxSegmentCount = Clamp(options.JournalMaxSegmentCount, WalSegmentLimits.DefaultMaxSegmentCount, WalSegmentLimits.HardMaxSegmentCount);
        _maxTotalBytes = ClampMb(options.JournalMaxTotalBytesMb, WalSegmentLimits.DefaultMaxTotalBytesMb, WalSegmentLimits.HardMaxTotalBytesMb);
    }

    public long MaxSegmentBytes => _maxSegmentBytes;

    public int MaxSegmentCount => _maxSegmentCount;

    public long MaxTotalBytes => _maxTotalBytes;

    public bool ShouldRollSegment(long activeSegmentWrittenBytes, int incomingFrameBytes) =>
        activeSegmentWrittenBytes + incomingFrameBytes > _maxSegmentBytes;

    public void EnsureCapacityOrThrow(int segmentCount, long totalBytes)
    {
        if (segmentCount > _maxSegmentCount)
        {
            throw new JournalCapacityExceededException(
                $"journal segment count {segmentCount.ToString(CultureInfo.InvariantCulture)} exceeds limit {_maxSegmentCount.ToString(CultureInfo.InvariantCulture)}.");
        }

        if (totalBytes > _maxTotalBytes)
        {
            throw new JournalCapacityExceededException(
                $"journal total bytes {totalBytes.ToString(CultureInfo.InvariantCulture)} exceed limit {_maxTotalBytes.ToString(CultureInfo.InvariantCulture)}.");
        }
    }

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
}
