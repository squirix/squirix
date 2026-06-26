using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Single-threaded journal I/O event loop: drains the ring, coalesces and writes frames, performs
/// segment rolls, and services group-commit durability deadlines. All members run on the dedicated
/// <c>squirix-journal-io</c> thread except the roll-completion signals invoked from the manifest-roll
/// thread and the property reads observed by callers.
/// </summary>
internal sealed class JournalEventLoop
{
    private readonly CancellationToken _bgToken;
    private readonly IJournalEventLoopHost _host;
    private readonly PersistenceOptions _opt;
    private readonly JournalSegmentPolicy _policy;
    private readonly BoundedJournalRing _ring;
    private readonly IJournalSegmentWriter _segmentWriter;
    private readonly JournalWriteBatchBuffer _writeBatch;
    private string? _activeSegmentPath;
    private long _activeSegmentWrittenBytes;
    private volatile bool _dirty;
    private JournalDurabilityGroupCommit? _groupCommit;
    private int _journalSegmentCount;
    private long _journalTotalBytes;
    private int _pendingRollTargetSegmentIndex;
    private int _segmentRollCompletionPending;
    private bool _segmentRollInFlight;

    internal JournalEventLoop(
        IJournalEventLoopHost host,
        BoundedJournalRing ring,
        IJournalSegmentWriter segmentWriter,
        PersistenceOptions opt,
        int currentSegmentIndex,
        long journalTotalBytes,
        int journalSegmentCount,
        CancellationToken bgToken)
    {
        _host = host;
        _ring = ring;
        _segmentWriter = segmentWriter;
        _opt = opt;
        _writeBatch = new JournalWriteBatchBuffer(opt.JournalWriteBatchBytes);
        _policy = new JournalSegmentPolicy(opt);
        CurrentSegmentIndex = currentSegmentIndex;
        _journalTotalBytes = journalTotalBytes;
        _journalSegmentCount = journalSegmentCount;
        _bgToken = bgToken;
    }

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    internal int CurrentSegmentIndex { get; private set; }

    internal bool IsDurabilityFlushPending => _dirty;

    internal void AttachGroupCommit(JournalDurabilityGroupCommit? groupCommit) => _groupCommit = groupCommit;

    internal void FlushGroupCommitOnJournalThread()
    {
        FlushWriteBatch();
        if (_dirty)
            FsyncOnJournalThread();
    }

    internal void FsyncOnJournalThread()
    {
        if (!_dirty)
            return;

        _segmentWriter.Fsync();
        _dirty = false;
    }

    internal void MarkRollAborted()
    {
        _segmentRollInFlight = false;
        Volatile.Write(ref _segmentRollCompletionPending, 0);
    }

    internal void MarkRollCompletionPending() => Volatile.Write(ref _segmentRollCompletionPending, 1);

    internal void Run()
    {
        try
        {
            JournalWorkItem? rollDeferredAppend = null;
            for (var running = true; running;)
                running = RunJournalThreadIteration(ref rollDeferredAppend);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or TimeoutException)
        {
            _host.FailPipeline(ex);
        }
        catch (OperationCanceledException) when (_bgToken.IsCancellationRequested)
        {
            // journal I/O thread exits when background cancellation is requested during dispose.
        }
    }

    private static void CompleteJournalWorkItem(JournalWorkItem item) => item.Completion?.SetResult();

    private void BeginSegmentRollOnJournalThread()
    {
        if (_segmentRollInFlight)
            return;

        FsyncOnJournalThread();

        // Roll capacity uses in-memory counters maintained by the single journal-thread writer instead
        // of rescanning the directory (two EnumerateFiles passes plus a stat per segment) on the hot
        // roll path. The counters are seeded at startup and resynced after compaction (MaintenanceEnd).
        _policy.EnsureRollCapacityOrThrow(_journalSegmentCount, _journalTotalBytes);
        _pendingRollTargetSegmentIndex = CurrentSegmentIndex + 1;
        _segmentRollInFlight = true;
        _host.PublishRoll(_pendingRollTargetSegmentIndex);
    }

    private void CompleteSegmentRollOnJournalThread()
    {
        CurrentSegmentIndex = _pendingRollTargetSegmentIndex;
        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        _segmentWriter.OpenSegment(_activeSegmentPath, false);
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        _segmentWriter.Write(header, 0);
        Volatile.Write(ref _activeSegmentWrittenBytes, JournalFraming.FileHeaderSize);
        _journalTotalBytes += JournalFraming.FileHeaderSize;
        _journalSegmentCount++;
        _dirty = false;
        _segmentRollInFlight = false;
    }

    private void CompleteStagedAppend(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            _host.DecrementQueuedAppends();
        }
        finally
        {
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }

        CompleteJournalWorkItem(item);
    }

    private void DrainDueGroupCommitBatches()
    {
        if (_groupCommit is null || _host.ReadQueuedAppends() > 0)
            return;

        _groupCommit.DrainDueBatchesOnJournalThread();
    }

    private void DrainDueGroupCommitBatchesDuringRoll()
    {
        // The queued-append gate is intentionally skipped here: during a roll the write batch is empty
        // and the previously written bytes are already fsynced, and the only queued appends are the
        // deferred frame plus any not-yet-staged frames whose producers are still blocked on their
        // Completion waiter and therefore cannot have registered a durability waiter yet. Every pending
        // group-commit waiter thus maps to an already-durable append.
        _groupCommit?.DrainDueBatchesOnJournalThread();
    }

    private bool DrainJournalRing(ref JournalWorkItem? rollDeferredAppend, out bool shutdownRequested)
    {
        shutdownRequested = false;
        var hadWork = false;
        while (_ring.TryDequeue(out var item))
        {
            hadWork = true;
            if (item.Kind is JournalWorkKind.Append)
            {
                if (TryAcceptAppendIntoBatch(item, out var rollDeferred))
                    continue;

                if (rollDeferred)
                {
                    rollDeferredAppend = item;
                    return hadWork;
                }

                FlushWriteBatch();
                _ = ProcessJournalWorkItem(item);
                continue;
            }

            FlushWriteBatch();
            if (!ProcessJournalWorkItem(item))
                continue;

            FlushWriteBatch();
            shutdownRequested = true;
            return hadWork;
        }

        return hadWork;
    }

    private void EnsureSegmentOpen()
    {
        if (_activeSegmentPath is not null)
        {
            // _activeSegmentWrittenBytes is authoritative: the journal thread is the sole writer and
            // advances it after every Write. No per-call stat/lseek of the writer length is needed.
            return;
        }

        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        var append = File.Exists(_activeSegmentPath);
        _segmentWriter.OpenSegment(_activeSegmentPath, append);
        if (_segmentWriter.Length == 0)
        {
            Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
            JournalFraming.WriteFileHeader(header);
            _segmentWriter.Write(header, 0);
        }

        _activeSegmentWrittenBytes = _segmentWriter.Length;
    }

    private void FlushWriteBatch()
    {
        if (_writeBatch.IsEmpty)
            return;

        var span = _writeBatch.ActiveSpan;
        var offset = _activeSegmentWrittenBytes;
        try
        {
            _segmentWriter.Write(span, offset);
        }
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }

        Volatile.Write(ref _activeSegmentWrittenBytes, offset + span.Length);
        _journalTotalBytes += span.Length;
        _dirty = true;

        for (var i = 0; i < _writeBatch.PendingAppends.Count; i++)
            CompleteStagedAppend(_writeBatch.PendingAppends[i].Item);

        _writeBatch.Clear();

        if (_opt.IsJournalGroupCommitEnabled)
            TryCompleteGroupCommitCheckpoint();
    }

    private long GetEffectiveActiveSegmentBytes() => _activeSegmentWrittenBytes + _writeBatch.StagedByteLength;

    private void ProcessAppend(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            WriteAppendFrame(item);
        }
        finally
        {
            _host.DecrementQueuedAppends();
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }

        if (_opt.IsJournalGroupCommitEnabled)
            TryCompleteGroupCommitCheckpoint();

        CompleteJournalWorkItem(item);
    }

    private void ProcessAppendWithDurability(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        var waiter = item.DurabilityWaiter ?? throw new InvalidOperationException("AppendWithDurability work item is missing a durability waiter.");
        try
        {
            WriteAppendFrame(item);
            FsyncOnJournalThread();
            _ = waiter.TrySetResult();
        }
        finally
        {
            _host.DecrementQueuedAppends();
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }
    }

    private bool ProcessJournalWorkItem(JournalWorkItem item)
    {
        switch (item.Kind)
        {
            case JournalWorkKind.Append:
                ProcessAppend(item);
                return false;

            case JournalWorkKind.AppendWithDurability:
                ProcessAppendWithDurability(item);
                return false;

            case JournalWorkKind.Flush:
            case JournalWorkKind.DurabilityCheckpoint:
                FlushWriteBatch();
                _host.CompleteDurabilityCheckpoint();
                return false;

            case JournalWorkKind.Shutdown:
                FlushWriteBatch();
                FsyncOnJournalThread();
                return true;

            case JournalWorkKind.MaintenanceBegin:
                FlushWriteBatch();
                FsyncOnJournalThread();
                _activeSegmentPath = null;
                CompleteJournalWorkItem(item);
                return false;

            case JournalWorkKind.MaintenanceEnd:
                CurrentSegmentIndex = item.ResetSegmentIndex;
                _host.SetNextSequence(item.ResetSequence);
                _activeSegmentWrittenBytes = 0;
                _dirty = false;

                // Compaction rewrote the segment set on disk; resync the in-memory capacity counters
                // (used by the hot roll path) from the new on-disk layout.
                var postMaintenanceStats = JournalReader.GetOnDiskSegmentStats(_opt.DataDir);
                _journalTotalBytes = postMaintenanceStats.TotalBytes;
                _journalSegmentCount = postMaintenanceStats.SegmentCount;
                CompleteJournalWorkItem(item);
                return false;

            default:
                throw new InvalidOperationException($"unknown journal work kind {item.Kind}.");
        }
    }

    private void ProcessRollDeferredAppend(ref JournalWorkItem? rollDeferredAppend)
    {
        var item = rollDeferredAppend ?? throw new InvalidOperationException("roll-deferred append is missing.");
        rollDeferredAppend = null;
        if (TryAcceptAppendIntoBatch(item, out var rollDeferred))
            return;

        if (rollDeferred)
        {
            rollDeferredAppend = item;
            return;
        }

        FlushWriteBatch();
        _ = ProcessJournalWorkItem(item);
    }

    private bool RunJournalThreadIteration(ref JournalWorkItem? rollDeferredAppend)
    {
        if (TryCompletePendingSegmentRoll() && rollDeferredAppend is not null)
        {
            ProcessRollDeferredAppend(ref rollDeferredAppend);
            return true;
        }

        if (rollDeferredAppend is not null)
        {
            _host.ThrowIfJournalThreadFailed();

            // A pending group-commit waiter can only exist for an append that already finished its
            // staged write (the producer is held on its Completion waiter until the frame is written),
            // so every such waiter is covered by the pre-roll fsync. Service due batches while the
            // roll's manifest fsync is in flight instead of starving them for the whole roll, and bound
            // the wait by the next group-commit deadline.
            DrainDueGroupCommitBatchesDuringRoll();
            var rollWaitMs = _groupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
            _ring.WaitForWork(rollWaitMs, _bgToken);
            DrainDueGroupCommitBatchesDuringRoll();
            return true;
        }

        var hadWork = DrainJournalRing(ref rollDeferredAppend, out var shutdownRequested);
        if (shutdownRequested)
            return false;

        if (rollDeferredAppend is not null)
            return true;

        FlushWriteBatch();
        DrainDueGroupCommitBatches();

        if (hadWork)
            return true;

        var timeoutMs = _host.ReadQueuedAppends() > 0 ? Timeout.Infinite : _groupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
        _ring.WaitForWork(timeoutMs, _bgToken);
        DrainDueGroupCommitBatches();
        return true;
    }

    private bool ShouldRollSegmentForAppend(int incomingFrameBytes) => _policy.ShouldRollSegment(GetEffectiveActiveSegmentBytes(), incomingFrameBytes);

    private void TruncateActiveSegmentAfterFailedFrame(long frameStart)
    {
        _segmentWriter.Truncate(frameStart);
        Volatile.Write(ref _activeSegmentWrittenBytes, frameStart);
        _dirty = frameStart > 0;
    }

    private bool TryAcceptAppendIntoBatch(JournalWorkItem item, out bool rollDeferred)
    {
        rollDeferred = false;
        EnsureSegmentOpen();
        _policy.EnsureAppendCapacityOrThrow(_journalTotalBytes, item.FrameLength);
        if (ShouldRollSegmentForAppend(item.FrameLength))
        {
            FlushWriteBatch();
            BeginSegmentRollOnJournalThread();
            rollDeferred = true;
            return false;
        }

        if (_writeBatch.TryStageAppend(in item))
            return true;

        FlushWriteBatch();
        return _writeBatch.TryStageAppend(in item);
    }

    private void TryCompleteGroupCommitCheckpoint() => DrainDueGroupCommitBatches();

    private bool TryCompletePendingSegmentRoll()
    {
        if (Volatile.Read(ref _segmentRollCompletionPending) is 0)
            return false;

        Volatile.Write(ref _segmentRollCompletionPending, 0);
        CompleteSegmentRollOnJournalThread();
        return true;
    }

    private void WriteAppendFrame(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        EnsureSegmentOpen();
        _policy.EnsureAppendCapacityOrThrow(_journalTotalBytes, item.FrameLength);
        if (ShouldRollSegmentForAppend(item.FrameLength))
            throw new InvalidOperationException("append requires a segment roll; use the journal thread deferral path.");
        var offset = _activeSegmentWrittenBytes;
        try
        {
            _segmentWriter.Write(frameBytes.AsSpan(0, item.FrameLength), offset);
        }
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }

        Volatile.Write(ref _activeSegmentWrittenBytes, offset + item.FrameLength);
        _journalTotalBytes += item.FrameLength;
        _dirty = true;
    }
}
