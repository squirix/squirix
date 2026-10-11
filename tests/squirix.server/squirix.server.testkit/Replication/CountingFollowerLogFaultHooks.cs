using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Counts the durable boundaries a follower log crosses: metadata publishes, frame writes, and flushes.</summary>
/// <remarks>
/// Counters are monotonic; take a <see cref="Snapshot" /> before and after the work under measurement and subtract. The log reports a
/// metadata publish once, after it wrote the new record and before it replaces the file, so a publish that fails at the replace is
/// counted too: the count is of publishes started, which equals publishes completed on a healthy log.
/// </remarks>
[ThreadSafe]
public sealed class CountingFollowerLogFaultHooks : IFollowerLogFaultHooks
{
    private long _flushes;
    private long _frameWrites;
    private long _metaWrites;

    /// <summary>Returns the counts observed so far.</summary>
    /// <returns>A point-in-time copy of the counters.</returns>
    public FollowerLogDurabilityCounts Snapshot() =>
        new(Interlocked.Read(ref _metaWrites), Interlocked.Read(ref _frameWrites), Interlocked.Read(ref _flushes));

    /// <inheritdoc />
    public void OnBeforeMemoryApply()
    {
    }

    /// <inheritdoc />
    public void OnCommitAdvanced()
    {
    }

    /// <inheritdoc />
    public void OnFlushed() => _ = Interlocked.Increment(ref _flushes);

    /// <inheritdoc />
    public void OnFrameWritten() => _ = Interlocked.Increment(ref _frameWrites);

    /// <inheritdoc />
    public void OnMetaWritten() => _ = Interlocked.Increment(ref _metaWrites);
}
