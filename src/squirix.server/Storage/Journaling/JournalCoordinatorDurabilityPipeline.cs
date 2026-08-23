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

    internal void CompleteDurabilityCheckpointOnJournalThread(JournalWorkItem item)
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
        }

        // Cancellation during fsync transfers pooling responsibility to this completion path
        // (MarkAbandonedByCaller): the terminal setter has now run, so the ack can safely be
        // pooled here. In every other case the caller pools it after consuming its result,
        // which keeps GetResult always ahead of any reset.
        if (ack.IsAbandonedByCaller())
            ack.ReturnToPool();

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    internal async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = DurabilityAck.Rent();
        try
        {
            var beginWaitTask = begin.AwaitAsync(cancellationToken);
            var beginItem = JournalWorkItem.MaintenanceBegin(begin);
            await _owner.Ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);

            await beginWaitTask.ConfigureAwait(false);
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _owner.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

            var end = DurabilityAck.Rent();
            try
            {
                var endWaitTask = end.AwaitAsync(cancellationToken);
                var endItem = JournalWorkItem.MaintenanceEnd(end, resetSegmentIndex, resetSequence);
                await _owner.Ring.EnqueueAsync(endItem, cancellationToken).ConfigureAwait(false);

                await endWaitTask.ConfigureAwait(false);
            }
            finally
            {
                end.ReturnToPool();
            }
        }
        finally
        {
            begin.ReturnToPool();
        }
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
        var acks = _owner.DurabilityAcks.Fail(reason);

        for (var i = 0; i < acks.Count; i++)
            _ = acks[i].TrySetException(reason);

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
        var ack = DurabilityAck.Rent();
        var enqueued = false;

        // Initialize the wait before exposing the ack to failure paths: a concurrent
        // FailPendingDurabilityAcks must never complete a source that this await would reset.
        var ackWaitTask = ack.AwaitAsync(cancellationToken);
        try
        {
            // Registration and terminal-failure detection are atomic (registry lock). When the
            // journal already failed, TryRegister completes ackWaitTask with the recorded failure
            // and the enqueue below is skipped.
            if (_owner.DurabilityAcks.TryRegister(ack))
            {
                var item = JournalWorkItem.DurabilityCheckpoint(ack);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;
            }

            await ackWaitTask.ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = _owner.DurabilityAcks.Remove(ack);

            // When the ring already accepted the checkpoint the queued work item still holds this
            // ack, so pooling must not happen here: transfer responsibility to the journal-thread
            // completion path, which pools it after its terminal setter has run.
            if (enqueued)
                ack.MarkAbandonedByCaller();

            throw;
        }
        finally
        {
            _ = _owner.DurabilityAcks.Remove(ack);

            // The caller pools in every case except a transferred ownership: normal completion,
            // registration rejection, and enqueue failures all end here with a quiescent ack.
            if (!ack.IsAbandonedByCaller())
                ack.ReturnToPool();
        }
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
