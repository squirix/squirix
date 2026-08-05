using System;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Ring drain and journal-thread scheduling for <see cref="JournalEventLoop" />.</summary>
internal sealed class JournalEventLoopDrainScheduler
{
    private readonly JournalEventLoop _owner;
    private readonly JournalEventLoopSegmentWriter _segmentWriter;

    internal JournalEventLoopDrainScheduler(JournalEventLoop owner, JournalEventLoopSegmentWriter segmentWriter)
    {
        _owner = owner;
        _segmentWriter = segmentWriter;
    }

    /// <summary>Drains due group-commit batches on the journal thread.</summary>
    /// <remarks>
    /// Waiters register only after their own append Completion fires, so every pending waiter already
    /// covers bytes written to the active segment. Other producers may still hold queued-append
    /// credits (ring slots not yet published, or frames still staged); those credits must not block
    /// durability for waiters that are already due — otherwise a cancel storm can leave the journal
    /// thread sleeping forever with an empty ring and starve the batch.
    /// </remarks>
    internal void DrainDueGroupCommitBatches() => _owner.GroupCommit?.DrainDueBatchesOnJournalThread();

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

        // Always honor an armed group-commit deadline, even when queued-append credits remain.
        // Credits can outlive an empty ring while a producer is between counter increment and publish.
        // An infinite sleep then depends on another notify and can strand waiters under cancel storms.
        var timeoutMs = _owner.GroupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
        _owner.Ring.WaitForWork(timeoutMs, _owner.BackgroundToken);
        DrainDueGroupCommitBatches();
        return true;
    }

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
        _ = _segmentWriter.ProcessJournalWorkItem(item, this);
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
        _ = _segmentWriter.ProcessJournalWorkItem(item, this);
        return false;
    }

    private bool TryProcessNonAppendFromRing(JournalWorkItem item, out bool shutdownRequested)
    {
        shutdownRequested = false;
        _segmentWriter.FlushWriteBatch();
        if (!_segmentWriter.ProcessJournalWorkItem(item, this))
            return false;

        _segmentWriter.FlushWriteBatch();
        shutdownRequested = true;
        return true;
    }
}
