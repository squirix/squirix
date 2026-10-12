using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Default <see cref="IMemoryUsageAccounting" /> implementation: reads entries and bytes from the local cache and counts admission rejections.</summary>
[ThreadSafe]
internal sealed class MemoryUsageAccounting : IMemoryUsageAccounting
{
    private readonly ILocalCacheStats _stats;
    private long _admissionRejections;

    /// <summary>Initializes a new instance of the <see cref="MemoryUsageAccounting" /> class.</summary>
    /// <param name="stats">The local cache whose stored entries and recorded bytes the accounting reports.</param>
    public MemoryUsageAccounting(ILocalCacheStats stats)
    {
        ArgumentNullException.ThrowIfNull(stats);
        _stats = stats;
    }

    /// <inheritdoc />
    public long ReadEntryCount() => _stats.EntryCount;

    /// <inheritdoc />
    public long ReadEstimatedBytes() => _stats.EstimatedBytes;

    /// <inheritdoc />
    public long ReadRejectedWriteCount() => Volatile.Read(ref _admissionRejections);

    /// <inheritdoc />
    public void RecordAdmissionRejection() => _ = Interlocked.Increment(ref _admissionRejections);
}
