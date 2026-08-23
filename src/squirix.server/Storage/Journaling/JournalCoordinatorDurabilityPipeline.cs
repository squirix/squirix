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

    internal JournalCoordinatorDurabilityPipeline(IJournalCoordinatorState owner, IJournalCoordinatorSnapshotState snapshot)
    {
        _owner = owner;
        _snapshot = snapshot;
    }

    private ILogger JournalLog => LogManager.GetLogger<JournalCoordinatorDurabilityPipeline>();

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
            LogManager.DurabilityJoinWaitCanceledOnDispose(JournalLog);
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
        var acks = _owner.DurabilityAcks.TakeAll();

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

    private void DetachDurabilityAck(DurabilityAck ack) => _ = _owner.DurabilityAcks.Remove(ack);

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var ack = DurabilityAck.Rent();
        _owner.DurabilityAcks.Add(ack);

        try
        {
            var ackWaitTask = ack.AwaitAsync(cancellationToken);
            var item = JournalWorkItem.DurabilityCheckpoint(ack);
            await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);

            await ackWaitTask.ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            RemoveDurabilityAck(ack, cancellationToken);
            throw;
        }
        finally
        {
            DetachDurabilityAck(ack);
            ack.ReturnToPool();
        }
    }

    private void RemoveDurabilityAck(DurabilityAck ack, CancellationToken cancellationToken)
    {
        if (!_owner.DurabilityAcks.Remove(ack))
            return;

        _ = ack.TrySetCanceled(cancellationToken);
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
