using System;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Ring drain and journal-thread scheduling for <see cref="JournalEventLoop" />.</summary>
internal sealed class JournalEventLoopDrainScheduler
{
    private readonly JournalEventLoop _owner;
    private readonly JournalEventLoopSegmentWriter _segmentWriter;

    public JournalEventLoopDrainScheduler(JournalEventLoop owner, JournalEventLoopSegmentWriter segmentWriter)
    {
        _owner = owner;
        _segmentWriter = segmentWriter;
    }

    internal void DrainDueGroupCommitBatches()
    {
        if (_owner.GroupCommit is null || _owner.Host.ReadQueuedAppends() > 0)
            return;

        _owner.GroupCommit.DrainDueBatchesOnJournalThread();
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

            // A pending group-commit waiter can only exist for an append that already finished its
            // staged write (the producer is held on its Completion waiter until the frame is written),
            // so every such waiter is covered by the pre-roll fsync. Service due batches while the
            // roll's manifest fsync is in flight instead of starving them for the whole roll, and bound
            // the wait by the next group-commit deadline.
            DrainDueGroupCommitBatchesDuringRoll();
            var rollWaitMs = _owner.GroupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
            _owner.Ring.WaitForWork(rollWaitMs, _owner.BackgroundToken);
            DrainDueGroupCommitBatchesDuringRoll();
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

        var timeoutMs = _owner.Host.ReadQueuedAppends() > 0 ? Timeout.Infinite : _owner.GroupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
        _owner.Ring.WaitForWork(timeoutMs, _owner.BackgroundToken);
        DrainDueGroupCommitBatches();
        return true;
    }

    private void DrainDueGroupCommitBatchesDuringRoll()
    {
        // The queued-append gate is intentionally skipped here: during a roll the write batch is empty
        // and the previously written bytes are already fsynced, and the only queued appends are the
        // deferred frame plus any not-yet-staged frames whose producers are still blocked on their
        // Completion waiter and therefore cannot have registered a durability waiter yet. Every pending
        // group-commit waiter thus maps to an already-durable append.
        _owner.GroupCommit?.DrainDueBatchesOnJournalThread();
    }

    private bool DrainJournalRing(ref JournalWorkItem? rollDeferredAppend, out bool shutdownRequested)
    {
        shutdownRequested = false;
        var hadWork = false;
        while (_owner.Ring.TryDequeue(out var item))
        {
            hadWork = true;
            if (item.Kind is JournalWorkKind.Append)
            {
                if (_segmentWriter.TryAcceptAppendIntoBatch(item, out var rollDeferred))
                    continue;

                if (rollDeferred)
                {
                    rollDeferredAppend = item;
                    return hadWork;
                }

                _segmentWriter.FlushWriteBatch();
                _ = _segmentWriter.ProcessJournalWorkItem(item, this);
                continue;
            }

            _segmentWriter.FlushWriteBatch();
            if (!_segmentWriter.ProcessJournalWorkItem(item, this))
                continue;

            _segmentWriter.FlushWriteBatch();
            shutdownRequested = true;
            return hadWork;
        }

        return hadWork;
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
        _ = _segmentWriter.ProcessJournalWorkItem(item, this);
    }
}
