using System;
using System.IO;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Single-threaded journal I/O event loop: drains the ring, coalesces and writes frames, performs
/// segment rolls, and services group-commit durability deadlines. All members run on the dedicated
/// <c>squirix-journal-io</c> thread except the roll-completion signals invoked from the manifest-roll
/// thread and the property reads observed by callers.
/// </summary>
internal sealed class JournalEventLoop
{
    private readonly JournalEventLoopSegmentWriter _segmentWriterOps;
    private long _activeSegmentWrittenBytes;
    private int _segmentRollCompletionPending;

    internal JournalEventLoop(
        IJournalEventLoopHost host,
        BoundedJournalRing ring,
        IJournalSegmentWriter segmentWriter,
        PersistenceOptions opt,
        JournalEventLoopStartup startup,
        CancellationToken bgToken)
    {
        Host = host;
        Ring = ring;
        SegmentWriter = segmentWriter;
        Options = opt;
        WriteBatch = new JournalWriteBatchBuffer(opt.JournalWriteBatchBytes);
        Policy = new JournalSegmentPolicy(opt);
        CurrentSegmentIndex = startup.CurrentSegmentIndex;
        JournalTotalBytes = startup.JournalTotalBytes;
        JournalSegmentCount = startup.JournalSegmentCount;
        BackgroundToken = bgToken;
        _segmentWriterOps = new JournalEventLoopSegmentWriter(this);
        DrainScheduler = new JournalEventLoopDrainScheduler(this, _segmentWriterOps);
    }

    internal string? ActiveSegmentPath { get; private set; }

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    internal CancellationToken BackgroundToken { get; }

    internal int CurrentSegmentIndex { get; private set; }

    internal JournalEventLoopDrainScheduler DrainScheduler { get; }

    internal JournalDurabilityGroupCommit? GroupCommit { get; private set; }

    internal IJournalEventLoopHost Host { get; }

    internal int JournalSegmentCount { get; private set; }

    internal long JournalTotalBytes { get; private set; }

    internal PersistenceOptions Options { get; }

    internal int PendingRollTargetSegmentIndex { get; private set; }

    internal JournalSegmentPolicy Policy { get; }

    internal BoundedJournalRing Ring { get; }

    internal ref int SegmentRollCompletionPendingField => ref _segmentRollCompletionPending;

    internal bool SegmentRollInFlight { get; private set; }

    internal IJournalSegmentWriter SegmentWriter { get; }

    internal JournalWriteBatchBuffer WriteBatch { get; }

    private bool IsDurabilityFlushPending { get; set; }

    internal void AddJournalTotalBytes(long delta) => JournalTotalBytes += delta;

    internal void AttachGroupCommit(JournalDurabilityGroupCommit? groupCommit) => GroupCommit = groupCommit;

    internal void FlushGroupCommitOnJournalThread()
    {
        _segmentWriterOps.FlushWriteBatch();
        if (IsDurabilityFlushPending)
            FsyncOnJournalThread();
    }

    internal void FsyncOnJournalThread()
    {
        if (!IsDurabilityFlushPending)
            return;

        SegmentWriter.Fsync();
        IsDurabilityFlushPending = false;
    }

    internal void IncrementJournalSegmentCount() => JournalSegmentCount++;

    internal void MarkRollAborted()
    {
        SegmentRollInFlight = false;
        Volatile.Write(ref _segmentRollCompletionPending, 0);
    }

    internal void MarkRollCompletionPending() => Volatile.Write(ref _segmentRollCompletionPending, 1);

    internal void Run()
    {
        try
        {
            JournalWorkItem? rollDeferredAppend = null;
            for (var running = true; running;)
                running = DrainScheduler.RunJournalThreadIteration(ref rollDeferredAppend);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or TimeoutException)
        {
            Host.FailPipeline(ex);
        }
        catch (OperationCanceledException) when (BackgroundToken.IsCancellationRequested)
        {
            // journal I/O thread exits when background cancellation is requested during dispose.
        }
    }

    internal void SetActiveSegmentPath(string? value) => ActiveSegmentPath = value;

    internal void SetActiveSegmentWrittenBytes(long value) => Volatile.Write(ref _activeSegmentWrittenBytes, value);

    internal void SetCurrentSegmentIndex(int value) => CurrentSegmentIndex = value;

    internal void SetDirty(bool value) => IsDurabilityFlushPending = value;

    internal void SetJournalSegmentCount(int value) => JournalSegmentCount = value;

    internal void SetJournalTotalBytes(long value) => JournalTotalBytes = value;

    internal void SetPendingRollTargetSegmentIndex(int value) => PendingRollTargetSegmentIndex = value;

    internal void SetSegmentRollInFlight(bool value) => SegmentRollInFlight = value;
}
