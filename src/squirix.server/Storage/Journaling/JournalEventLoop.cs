using System;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Single-threaded journal I/O event loop: drains the ring, coalesces and writes frames, performs
/// segment rolls, and services group-commit durability deadlines. All members run on the dedicated
/// <c language="csharp">squirix-journal-io</c> thread except the roll-completion signals invoked from the manifest-roll
/// thread and the property reads observed by callers.
/// </summary>
internal sealed class JournalEventLoop : IJournalEventLoopState, IJournalEventLoopDrainState, IJournalEventLoopRollState
{
    private readonly JournalEventLoopSegmentWriter _segmentWriterOps;
    private readonly JournalSlowOperationReporter _slowOperations;
    private long _activeSegmentWrittenBytes;
    private int _journalSegmentCount;
    private long _journalTotalBytes;
    private int _openCreatesSegment;
    private int _rollTargetCounted;
    private int _segmentRollCompletionPending;

    internal JournalEventLoop(
        IJournalEventLoopHost host,
        BoundedJournalRing ring,
        IJournalSegmentWriter segmentWriter,
        PersistenceOptions opt,
        JournalEventLoopStartup startup,
        ILogger<JournalEventLoop> logger,
        CancellationToken bgToken)
    {
        ArgumentNullException.ThrowIfNull(logger);
        JournalLog = logger;

        // Only the slow-fsync warning reads this clock, and it times real disk I/O: the system clock, never a test clock.
        _slowOperations = new JournalSlowOperationReporter(logger, TimeProvider.System);
        Host = host;
        Ring = ring;
        SegmentWriter = segmentWriter;
        Options = opt;
        WriteBatch = new JournalWriteBatchBuffer(opt.JournalWriteBatch);
        Policy = new JournalSegmentPolicy(opt);
        CurrentSegmentIndex = startup.CurrentSegmentIndex;
        _journalTotalBytes = startup.JournalTotalBytes;
        _journalSegmentCount = startup.JournalSegmentCount;

        // Producers read this counter to predict a segment roll before the journal thread opens the segment, so it must already
        // equal what EnsureSegmentOpen will set (the on-disk length, or a header for a missing or empty file) instead of zero.
        // Refusing every append while the segment is not open would refuse forever at the segment-count limit (a refused append
        // never opens the segment), and opening the segment eagerly would create a segment file and header with nothing to write.
        _activeSegmentWrittenBytes = startup.ActiveSegment.ActiveBytesAfterOpen;
        _openCreatesSegment = startup.ActiveSegment.OpenCreatesSegment ? 1 : 0;
        _rollTargetCounted = startup.ActiveSegment.RollTargetCounted ? 1 : 0;
        BackgroundToken = bgToken;
        _segmentWriterOps = new JournalEventLoopSegmentWriter(this, this);
        DrainScheduler = new JournalEventLoopDrainScheduler(this, _segmentWriterOps);
    }

    public string? ActiveSegmentPath { get; private set; }

    long IJournalEventLoopState.ActiveSegmentWrittenBytes => ActiveSegmentWrittenBytes;

    public CancellationToken BackgroundToken { get; }

    public int CurrentSegmentIndex { get; private set; }

    public JournalDurabilityGroupCommit? GroupCommit { get; private set; }

    public IJournalEventLoopHost Host { get; }

    /// <summary>Gets the on-disk journal segment count. Written only by the journal thread; read cross-thread.</summary>
    public int JournalSegmentCount => Volatile.Read(ref _journalSegmentCount);

    /// <summary>Gets the on-disk journal byte total. Written only by the journal thread; read cross-thread.</summary>
    public long JournalTotalBytes => Volatile.Read(ref _journalTotalBytes);

    /// <summary>
    /// Gets a value indicating whether the next segment open creates the missing current segment file and counts it. Cleared by the
    /// journal thread only after that count was added; read cross-thread by append admission.
    /// </summary>
    public bool OpenCreatesSegment => Volatile.Read(ref _openCreatesSegment) != 0;

    public PersistenceOptions Options { get; }

    public int PendingRollTargetSegmentIndex { get; private set; }

    public JournalSegmentPolicy Policy { get; }

    public BoundedJournalRing Ring { get; }

    /// <summary>
    /// Gets a value indicating whether the next roll target is already on disk and counted with at most a file header, so the next roll
    /// adds no segment. Cleared by the journal thread when a roll begins; read cross-thread by append admission.
    /// </summary>
    public bool RollTargetCounted => Volatile.Read(ref _rollTargetCounted) != 0;

    public bool SegmentRollInFlight { get; private set; }

    public IJournalSegmentWriter SegmentWriter { get; }

    public JournalWriteBatchBuffer WriteBatch { get; }

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    private JournalEventLoopDrainScheduler DrainScheduler { get; }

    private bool IsDurabilityFlushPending { get; set; }

    private ILogger JournalLog { get; }

    public void AddJournalTotalBytes(long delta) => Volatile.Write(ref _journalTotalBytes, _journalTotalBytes + delta);

    public void FlushToDisk()
    {
        if (!IsDurabilityFlushPending)
            return;

        var startedTimestamp = _slowOperations.GetTimestamp();
        try
        {
            SegmentWriter.FlushToDisk();
            IsDurabilityFlushPending = false;
        }
        finally
        {
            _slowOperations.ReportFsync(startedTimestamp);
        }
    }

    public void IncrementJournalSegmentCount() => Volatile.Write(ref _journalSegmentCount, _journalSegmentCount + 1);

    public void MarkSegmentRollCompletionPending() => Volatile.Write(ref _segmentRollCompletionPending, 1);

    public void SetActiveSegmentPath(string? value) => ActiveSegmentPath = value;

    public void SetActiveSegmentWrittenBytes(long value) => Volatile.Write(ref _activeSegmentWrittenBytes, value);

    public void SetCurrentSegmentIndex(int value) => CurrentSegmentIndex = value;

    public void SetDirty(bool value) => IsDurabilityFlushPending = value;

    public void SetJournalSegmentCount(int value) => Volatile.Write(ref _journalSegmentCount, value);

    public void SetJournalTotalBytes(long value) => Volatile.Write(ref _journalTotalBytes, value);

    public void SetOpenCreatesSegment(bool value) => Volatile.Write(ref _openCreatesSegment, value ? 1 : 0);

    public void SetPendingRollTargetSegmentIndex(int value) => PendingRollTargetSegmentIndex = value;

    public void SetRollTargetCounted(bool value) => Volatile.Write(ref _rollTargetCounted, value ? 1 : 0);

    public void SetSegmentRollInFlight(bool value) => SegmentRollInFlight = value;

    public bool TryConsumeSegmentRollCompletion()
    {
        if (Volatile.Read(ref _segmentRollCompletionPending) == 0)
            return false;

        Volatile.Write(ref _segmentRollCompletionPending, 0);
        return true;
    }

    internal void AttachGroupCommit(JournalDurabilityGroupCommit? groupCommit) => GroupCommit = groupCommit;

    internal void FlushGroupCommitOnJournalThread()
    {
        _segmentWriterOps.FlushWriteBatch();
        if (IsDurabilityFlushPending)
            FlushToDisk();
    }

    internal void MarkRollAborted()
    {
        SegmentRollInFlight = false;
        Volatile.Write(ref _segmentRollCompletionPending, 0);
    }

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
            ServerLog.JournalThreadExitOnCancel(JournalLog);
        }
    }

    /// <summary>Ring drain and journal-thread scheduling for the journal event loop.</summary>
    [Immutable]
    private sealed class JournalEventLoopDrainScheduler
    {
        private readonly IJournalEventLoopDrainState _owner;
        private readonly JournalEventLoopSegmentWriter _segmentWriter;

        internal JournalEventLoopDrainScheduler(
            IJournalEventLoopDrainState owner,
            JournalEventLoopSegmentWriter segmentWriter)
        {
            _owner = owner;
            _segmentWriter = segmentWriter;
        }

        internal bool RunJournalThreadIteration(ref JournalWorkItem? rollDeferredAppend)
        {
            if (_segmentWriter.TryCompletePendingSegmentRoll() && rollDeferredAppend != null)
            {
                ProcessRollDeferredAppend(ref rollDeferredAppend);
                return true;
            }

            if (rollDeferredAppend != null)
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

            if (rollDeferredAppend != null)
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
            if (item.Kind != JournalWorkKind.Append)
                return TryProcessNonAppendFromRing(item, out shutdownRequested);
            shutdownRequested = false;
            return TryProcessAppendFromRing(item, ref rollDeferredAppend);
        }

        private void ProcessRollDeferredAppend(ref JournalWorkItem? rollDeferredAppend)
        {
            var item = ThrowHelper.Required(rollDeferredAppend, "roll-deferred append is missing.");
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
