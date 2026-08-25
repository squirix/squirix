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
internal sealed class JournalDurabilityCoordinator
{
    private readonly IJournalCoordinatorState _owner;
    private readonly IJournalCoordinatorSnapshotState _snapshot;
    private readonly ILogger _logger;

    internal JournalDurabilityCoordinator(IJournalCoordinatorState owner, IJournalCoordinatorSnapshotState snapshot, ILogger logger)
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
        var completion = item.Completion ?? throw new InvalidOperationException("durability checkpoint work item is missing a completion source.");

        // Complete only the source carried by this work item. The source rides the ring position of its
        // own checkpoint, so a flush performed here is guaranteed to cover every frame enqueued before
        // it. Completing sources registered later (their checkpoints are still queued behind this item)
        // would report frames durable before they are written, so foreign waits must stay pending.
        // The fsync runs even when the caller canceled its wait: the checkpoint was admitted, so the
        // staged frames must reach disk regardless of whether anyone still observes the wait.
        _owner.EventLoop.FsyncOnJournalThread();
        _ = completion.TrySetResult();

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    internal async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = DurabilityAckRegistry.NewWait();
        _owner.DurabilityAcks.Add(begin);
        try
        {
            await _owner.Ring.EnqueueAsync(JournalWorkItem.MaintenanceBegin(begin), cancellationToken).ConfigureAwait(false);

            await begin.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _owner.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

            var end = DurabilityAckRegistry.NewWait();
            _owner.DurabilityAcks.Add(end);
            try
            {
                await _owner.Ring.EnqueueAsync(JournalWorkItem.MaintenanceEnd(end, resetSegmentIndex, resetSequence), cancellationToken).ConfigureAwait(false);

                await end.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                DetachDurabilityWait(end);
            }
        }
        finally
        {
            DetachDurabilityWait(begin);
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
        var waits = _owner.DurabilityAcks.FailAll(reason);

        for (var i = 0; i < waits.Count; i++)
            _ = waits[i].TrySetException(reason);

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

    internal async ValueTask<AsyncLockHolder> WaitForSnapshotCutAdmissionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await _snapshot.InFlightApplyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            var gateGuard = await _snapshot.MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
            if (!_snapshot.InFlightApplyGate.HasPending)
                return gateGuard;

            gateGuard.Dispose();
        }
    }

    private void DetachDurabilityWait(TaskCompletionSource completion) => _ = _owner.DurabilityAcks.Remove(completion);

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var completion = DurabilityAckRegistry.NewWait();
        _owner.DurabilityAcks.Add(completion);

        try
        {
            await _owner.Ring.EnqueueAsync(JournalWorkItem.DurabilityCheckpoint(completion), cancellationToken).ConfigureAwait(false);

            await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        finally
        {
            DetachDurabilityWait(completion);
        }
    }

    private sealed class JoinJournalThreadWork : IWorkPoolItem
    {
        private readonly JournalDurabilityCoordinator _pipeline;

        internal JoinJournalThreadWork(JournalDurabilityCoordinator pipeline)
        {
            _pipeline = pipeline;
        }

        internal bool Joined { get; private set; }

        void IWorkPoolItem.Execute() => Joined = _pipeline._owner.JournalThread.Join(TimeSpan.FromSeconds(30));
    }
}
