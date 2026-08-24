using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Durability, maintenance, and failure handling for a journal coordinator.</summary>
[Immutable]
internal sealed class JournalCoordinatorDurabilityPipeline
{
    private readonly IJournalCoordinatorState _owner;
    private readonly IJournalCoordinatorSnapshotState _snapshot;
    private readonly ILogger _logger;

    internal JournalCoordinatorDurabilityPipeline(IJournalCoordinatorState owner, IJournalCoordinatorSnapshotState snapshot, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _owner = owner;
        _snapshot = snapshot;
        _logger = logger;
    }

    internal static void ThrowDisposeFailures(List<Exception> failures)
    {
        switch (failures.Count)
        {
            case 0:
                return;
            case 1:
                ExceptionDispatchInfo.Capture(failures[0]).Throw();
                break;
            default:
                throw new AggregateException("journal coordinator disposal failed.", failures);
        }
    }

    internal async ValueTask AwaitJournalThreadDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
            var work = new JoinJournalThreadWork(this);
            await WorkPool.RunAsync(work, TaskCreationOptions.LongRunning, _owner.BackgroundCancellation.Token).ConfigureAwait(false);
            if (!work.Joined)
                failures.Add(new TimeoutException("journal I/O thread did not exit within 30 seconds."));
        }
        catch (OperationCanceledException) when (_owner.BackgroundCancellation.IsCancellationRequested)
        {
            // Dispose Canceled the join wait when teardown already completed.
            LogManager.DurabilityJoinWaitCanceledOnDispose(_logger);
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
    }

    internal void CompleteCheckpointOnJournalThread(JournalWorkItem item)
    {
        var ack = item.Ack ?? throw new InvalidOperationException("durability checkpoint work item is missing a durability ack.");

        // Complete only the ack carried by this work item. The ack rides the ring position of its
        // own checkpoint, so a flush performed here is guaranteed to cover every frame enqueued before
        // it. Completing acks registered later (their checkpoints are still queued behind this item)
        // would report frames durable before they are written, so foreign acks must stay pending.
        if (_owner.DurabilityAcks.Remove(ack))
        {
            _owner.EventLoop.FsyncOnJournalThread();
            _ = ack.TrySetResult();
            ack.Return();
        }

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    internal async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        // Ownership transfers to the journal once the work item is accepted by the ring; after that the
        // journal returns the ack to the pool (via CompleteJournalWorkItem) strictly after its terminal
        // setter. The caller only owns an ack that was not accepted, and returns it on that path.
        var begin = PooledAck.Rent();
        var beginWaitTask = begin.WaitAsync(cancellationToken);
        var beginItem = JournalWorkItem.MaintenanceBegin(begin);
        try
        {
            await _owner.Ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            begin.Return();
            throw;
        }

        await beginWaitTask.ConfigureAwait(false);
        await action(cancellationToken).ConfigureAwait(false);

        var manifest = await _owner.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

        var end = PooledAck.Rent();
        var endWaitTask = end.WaitAsync(cancellationToken);
        var endItem = JournalWorkItem.MaintenanceEnd(end, resetSegmentIndex, resetSequence);
        try
        {
            await _owner.Ring.EnqueueAsync(endItem, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            end.Return();
            throw;
        }

        await endWaitTask.ConfigureAwait(false);
    }

    internal ValueTask EnqueueShutdownAsync() => _owner.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

    internal void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        _owner.SetJournalThreadFailure(reason);
        FailPendingDurabilityAcks(reason);
        _owner.GroupCommit?.CancelPendingCore(reason);
    }

    internal void FailPendingDurabilityAcks(Exception reason)
    {
        var acks = _owner.DurabilityAcks.TakeAll();

        for (var i = 0; i < acks.Count; i++)
            _ = acks[i].TrySetException(reason);

        // Ownership has already transferred to the journal for accepted items, but those items are still
        // queued: the registry drained them, so the journal completion path will find nothing to complete
        // and must not pool again. Returning the ack here is the single pool point for the drained acks.
        for (var i = 0; i < acks.Count; i++)
            acks[i].Return();

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    internal ValueTask FlushAsync(CancellationToken cancellationToken) => EnqueueFlushAsync(cancellationToken);

    internal void OnManifestRollFailed(Exception ex)
    {
        _owner.EventLoop.MarkRollAborted();
        FailJournalPipeline(ex);
        _owner.Ring.NotifyWorkAvailable();
    }

    internal void OnManifestRollSucceeded()
    {
        _owner.EventLoop.MarkSegmentRollCompletionPending();
        _owner.Ring.NotifyWorkAvailable();
    }

    internal void ThrowIfJournalThreadFailed()
    {
        if (_owner.GetJournalThreadFailure() is { } failure)
            throw new InvalidOperationException("journal I/O thread failed.", failure);
    }

    internal async ValueTask WaitForSnapshotCutAdmissionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await _snapshot.InFlightApplyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            await _snapshot.MutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!_snapshot.InFlightApplyGate.HasPending)
                return;

            _ = _snapshot.MutationGate.Release();
        }
    }

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var ack = PooledAck.Rent();
        var task = ack.WaitAsync(cancellationToken);
        var item = JournalWorkItem.DurabilityCheckpoint(ack);

        try
        {
            _owner.DurabilityAcks.Add(ack);
            await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The item was not accepted by the ring, so the caller still owns the ack and is responsible
            // for returning it. The journal never observes it, so there is no double-pool.
            _ = _owner.DurabilityAcks.Remove(ack);
            _ = ack.TrySetCanceled(cancellationToken);
            ack.Return();
            throw;
        }

        await task.ConfigureAwait(false);
        ThrowIfJournalThreadFailed();
    }

    private sealed class JoinJournalThreadWork : IWorkPoolItem
    {
        private readonly JournalCoordinatorDurabilityPipeline _pipeline;

        internal JoinJournalThreadWork(JournalCoordinatorDurabilityPipeline pipeline)
        {
            _pipeline = pipeline;
        }

        internal bool Joined { get; private set; }

        void IWorkPoolItem.Execute() => Joined = _pipeline._owner.JournalThread.Join(TimeSpan.FromSeconds(30));
    }
}
