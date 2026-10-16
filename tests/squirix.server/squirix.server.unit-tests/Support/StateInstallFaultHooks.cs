using System.IO;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Follower-log fault hooks that fail one chosen metadata or snapshot slot write, or the next log flush, after the test arms them.</summary>
/// <remarks>
/// Models the crash points of a state install: a metadata publish counts as one slot write, so a mirrored record has a first and a second,
/// and so does a snapshot publish, which writes its two slot files one after the other.
/// </remarks>
[ThreadSafe]
internal sealed class StateInstallFaultHooks : IFollowerLogFaultHooks
{
    private CancellationTokenSource? _cancelOnFlush;
    private int _failFlush = -1;
    private int _flushes;
    private int _failPublish = -1;
    private int _failSnapshotPublish = -1;
    private int _publishes;
    private int _snapshotPublishes;

    /// <inheritdoc />
    public void OnBeforeMemoryApply()
    {
    }

    /// <inheritdoc />
    public void OnCommitAdvanced()
    {
    }

    /// <inheritdoc />
    public void OnFlushed()
    {
        Interlocked.Exchange(ref _cancelOnFlush, null)?.Cancel();
        if (Interlocked.Increment(ref _flushes) == Volatile.Read(ref _failFlush))
            throw new IOException("simulated failure at a log flush.");
    }

    /// <inheritdoc />
    public void OnFrameWritten()
    {
    }

    /// <inheritdoc />
    public void OnMetaPublishing()
    {
        if (Interlocked.Increment(ref _publishes) == Volatile.Read(ref _failPublish))
            throw new IOException("simulated failure at a metadata slot write.");
    }

    /// <inheritdoc />
    public void OnSnapshotPublishing()
    {
        if (Interlocked.Increment(ref _snapshotPublishes) == Volatile.Read(ref _failSnapshotPublish))
            throw new IOException("simulated failure at a snapshot slot write.");
    }

    /// <summary>Cancels a source at the next flush, without failing it: the caller's token is cancelled while the operation runs.</summary>
    /// <param name="source">The source to cancel.</param>
    internal void CancelOnNextFlush(CancellationTokenSource source) => Volatile.Write(ref _cancelOnFlush, source);

    /// <summary>Arms the fault for a metadata slot write counted from now.</summary>
    /// <param name="ordinal">One for the next slot write, two for the one after it.</param>
    internal void FailSlotWrite(int ordinal) => Volatile.Write(ref _failPublish, Volatile.Read(ref _publishes) + ordinal);

    /// <summary>Arms the fault for a snapshot slot write counted from now.</summary>
    /// <param name="ordinal">One for the next slot write, two for the one after it.</param>
    internal void FailSnapshotSlotWrite(int ordinal) => Volatile.Write(ref _failSnapshotPublish, Volatile.Read(ref _snapshotPublishes) + ordinal);

    /// <summary>Arms the fault for a flush of the log file counted from now.</summary>
    /// <param name="ordinal">One for the next flush, two for the one after it.</param>
    internal void FailFlush(int ordinal) => Volatile.Write(ref _failFlush, Volatile.Read(ref _flushes) + ordinal);

    /// <summary>Arms the fault for the next flush of the log file.</summary>
    internal void FailNextFlush() => FailFlush(1);
}
