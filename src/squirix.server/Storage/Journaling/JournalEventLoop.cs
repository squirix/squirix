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
internal sealed class JournalEventLoop : IJournalEventLoopState, IJournalEventLoopDrainState, IJournalEventLoopRollState
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
        _segmentWriterOps = new JournalEventLoopSegmentWriter(this, this);
        DrainScheduler = new JournalEventLoopDrainScheduler(this, _segmentWriterOps);
    }

    public string? ActiveSegmentPath { get; private set; }

    public CancellationToken BackgroundToken { get; }

    public int CurrentSegmentIndex { get; private set; }

    public JournalDurabilityGroupCommit? GroupCommit { get; private set; }

    public IJournalEventLoopHost Host { get; }

    public int JournalSegmentCount { get; private set; }

    public long JournalTotalBytes { get; private set; }

    public PersistenceOptions Options { get; }

    public int PendingRollTargetSegmentIndex { get; private set; }

    public JournalSegmentPolicy Policy { get; }

    public BoundedJournalRing Ring { get; }

    public ref int SegmentRollCompletionPendingField => ref _segmentRollCompletionPending;

    public bool SegmentRollInFlight { get; private set; }

    public IJournalSegmentWriter SegmentWriter { get; }

    public JournalWriteBatchBuffer WriteBatch { get; }

    long IJournalEventLoopState.ActiveSegmentWrittenBytes => ActiveSegmentWrittenBytes;

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    private JournalEventLoopDrainScheduler DrainScheduler { get; }

    private bool IsDurabilityFlushPending { get; set; }

    public void AddJournalTotalBytes(long delta) => JournalTotalBytes += delta;

    public void FsyncOnJournalThread()
    {
        if (!IsDurabilityFlushPending)
            return;

        SegmentWriter.Fsync();
        IsDurabilityFlushPending = false;
    }

    public void IncrementJournalSegmentCount() => JournalSegmentCount++;

    public void SetActiveSegmentPath(string? value) => ActiveSegmentPath = value;

    public void SetActiveSegmentWrittenBytes(long value) => Volatile.Write(ref _activeSegmentWrittenBytes, value);

    public void SetCurrentSegmentIndex(int value) => CurrentSegmentIndex = value;

    public void SetDirty(bool value) => IsDurabilityFlushPending = value;

    public void SetJournalSegmentCount(int value) => JournalSegmentCount = value;

    public void SetJournalTotalBytes(long value) => JournalTotalBytes = value;

    public void SetPendingRollTargetSegmentIndex(int value) => PendingRollTargetSegmentIndex = value;

    public void SetSegmentRollInFlight(bool value) => SegmentRollInFlight = value;

    internal void AttachGroupCommit(JournalDurabilityGroupCommit? groupCommit) => GroupCommit = groupCommit;

    internal void FlushGroupCommitOnJournalThread()
    {
        _segmentWriterOps.FlushWriteBatch();
        if (IsDurabilityFlushPending)
            FsyncOnJournalThread();
    }

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

    /// <summary>Ring drain and journal-thread scheduling for the journal event loop.</summary>
    private sealed class JournalEventLoopDrainScheduler
    {
        private readonly IJournalEventLoopDrainState _owner;
        private readonly JournalEventLoopSegmentWriter _segmentWriter;

        internal JournalEventLoopDrainScheduler(IJournalEventLoopDrainState owner, JournalEventLoopSegmentWriter segmentWriter)
        {
            _owner = owner;
            _segmentWriter = segmentWriter;
        }

        internal bool RunJournalThreadIteration(ref JournalWorkItem? rollDeferredAppend)
        {
            if (_segmentWriter.TryCompletePendingSegmentRoll() && rollDeferredAppend is not null)
            {
                ProcessRollDeferredAppend(ref rollDeferredAppend);
                return true;
            }

            if (rollDeferredAppend is not null)
            {
                _owner.Host.ThrowIfJournalThreadFailed();
                DrainDueGroupCommitBatches();
                var rollWaitMs = _owner.GroupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
                _owner.Ring.WaitForWork(rollWaitMs, _owner.BackgroundToken);
                DrainDueGroupCommitBatches();
                return true;
            }

            var hadWork = DrainJournalRing(ref rollDeferredAppend, out var shutdownRequested);
            if (shutdownRequested)
                return false;

            if (rollDeferredAppend is not null)
                return true;

            _segmentWriter.FlushWriteBatch(true);
            DrainDueGroupCommitBatches();

            if (hadWork)
                return true;

            var timeoutMs = _owner.GroupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
            _owner.Ring.WaitForWork(timeoutMs, _owner.BackgroundToken);
            DrainDueGroupCommitBatches();
            return true;
        }

        private void DrainDueGroupCommitBatches() => _owner.GroupCommit?.DrainDueBatchesOnJournalThread();

        private bool DrainJournalRing(ref JournalWorkItem? rollDeferredAppend, out bool shutdownRequested)
        {
            shutdownRequested = false;
            var hadWork = false;
            while (_owner.Ring.TryDequeue(out var item))
            {
                hadWork = true;
                if (ProcessRingItem(item, ref rollDeferredAppend, out shutdownRequested))
                    return hadWork;
            }

            return hadWork;
        }

        private bool ProcessRingItem(JournalWorkItem item, ref JournalWorkItem? rollDeferredAppend, out bool shutdownRequested)
        {
            if (item.Kind is not JournalWorkKind.Append)
                return TryProcessNonAppendFromRing(item, out shutdownRequested);
            shutdownRequested = false;
            return TryProcessAppendFromRing(item, ref rollDeferredAppend);
        }

        private void ProcessRollDeferredAppend(ref JournalWorkItem? rollDeferredAppend)
        {
            var item = rollDeferredAppend ?? throw new InvalidOperationException("roll-deferred append is missing.");
            rollDeferredAppend = null;
            if (_segmentWriter.TryAcceptAppendIntoBatch(item, out var rollDeferred))
                return;

            if (rollDeferred)
            {
                rollDeferredAppend = item;
                return;
            }

            _segmentWriter.FlushWriteBatch();
            _ = _segmentWriter.ProcessJournalWorkItem(item);
        }

        private bool TryProcessAppendFromRing(JournalWorkItem item, ref JournalWorkItem? rollDeferredAppend)
        {
            if (_segmentWriter.TryAcceptAppendIntoBatch(item, out var rollDeferred))
                return false;

            if (rollDeferred)
            {
                rollDeferredAppend = item;
                return true;
            }

            _segmentWriter.FlushWriteBatch();
            _ = _segmentWriter.ProcessJournalWorkItem(item);
            return false;
        }

        private bool TryProcessNonAppendFromRing(JournalWorkItem item, out bool shutdownRequested)
        {
            shutdownRequested = false;
            _segmentWriter.FlushWriteBatch();
            if (!_segmentWriter.ProcessJournalWorkItem(item))
                return false;

            _segmentWriter.FlushWriteBatch();
            shutdownRequested = true;
            return true;
        }
    }
}
