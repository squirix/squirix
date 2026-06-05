using System;
using System.Threading;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Default <see cref="IMemoryUsageAccounting" /> implementation using interlocked operations.
/// </summary>
internal sealed class MemoryUsageAccounting : IMemoryUsageAccounting
{
    private long _admissionRejections;

    private long _entryCount;

    private long _estimatedBytes;

    /// <inheritdoc />
    public long EntryCount => Volatile.Read(ref _entryCount);

    /// <inheritdoc />
    public long EstimatedBytes => Volatile.Read(ref _estimatedBytes);

    /// <inheritdoc />
    public long RejectedWriteCount => Volatile.Read(ref _admissionRejections);

    /// <inheritdoc />
    public void AddEntry(long estimatedBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(estimatedBytes);

        _ = Interlocked.Add(ref _estimatedBytes, estimatedBytes);
        _ = Interlocked.Increment(ref _entryCount);
    }

    /// <inheritdoc />
    public void RecordAdmissionRejection() => _ = Interlocked.Increment(ref _admissionRejections);

    /// <inheritdoc />
    public void RemoveEntry(long estimatedBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(estimatedBytes);

        SaturatingAdd(ref _estimatedBytes, -estimatedBytes);

        while (true)
        {
            var cur = Volatile.Read(ref _entryCount);
            var next = cur - 1;
            if (next < 0)
                next = 0;

            if (Interlocked.CompareExchange(ref _entryCount, next, cur) == cur)
                break;
        }
    }

    /// <inheritdoc />
    public void ReplaceEntry(long oldEstimatedBytes, long newEstimatedBytes)
    {
        var delta = newEstimatedBytes - oldEstimatedBytes;
        if (delta == 0)
            return;

        SaturatingAdd(ref _estimatedBytes, delta);
    }

    private static void SaturatingAdd(ref long field, long delta)
    {
        while (true)
        {
            var cur = Volatile.Read(ref field);
            var next = cur + delta;
            if (next < 0)
                next = 0;

            if (Interlocked.CompareExchange(ref field, next, cur) == cur)
                break;
        }
    }
}
