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
    private readonly ILogger _logger;
    private readonly IJournalCoordinatorState _owner;
    private readonly JournalProducerGate _producerGate;
    private readonly IJournalCoordinatorSnapshotState _snapshot;

    internal JournalDurabilityCoordinator(IJournalCoordinatorState owner, IJournalCoordinatorSnapshotState snapshot, ILogger logger, JournalProducerGate producerGate)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(producerGate);
        _owner = owner;
        _snapshot = snapshot;
        _logger = logger;
        _producerGate = producerGate;
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

    internal async ValueTask AwaitJournalThreadDuringDisposeAsync(List<Exception> failures, TimeSpan timeout)
    {
        try
        {
            var work = new JoinJournalThreadWork(this, timeout);
            await WorkPool.RunAsync(work, TaskCreationOptions.LongRunning, _owner.BackgroundCancellation.Token).ConfigureAwait(false);
            if (!work.Joined)
            {
                LogManager.JournalThreadJoinTimedOut(_logger);
                failures.Add(new TimeoutException($"journal I/O thread did not exit within {timeout}."));
            }
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
        var ack = ThrowHelper.Required(item.Ack, "durability checkpoint work item is missing a durability ack.");

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
        var begin = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _producerGate.Enter();
        try
        {
            _producerGate.ThrowIfShutdownInitiated();
            var beginItem = JournalWorkItem.MaintenanceBegin(begin);
            await _owner.Ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _producerGate.Exit();
        }

        await begin.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        await action(cancellationToken).ConfigureAwait(false);

        var manifest = await _owner.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

        var end = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _producerGate.Enter();
        try
        {
            _producerGate.ThrowIfShutdownInitiated();
            var endItem = JournalWorkItem.MaintenanceEnd(end, resetSegmentIndex, resetSequence);
            await _owner.Ring.EnqueueAsync(endItem, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _producerGate.Exit();
        }

        await end.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Enqueues the shutdown marker, giving up after the remaining budget.</summary>
    /// <param name="failures">Disposal failures to record a marker timeout into.</param>
    /// <param name="remaining">Time left in the shared shutdown budget.</param>
    /// <returns>A task that completes when the marker entered the ring or timed out.</returns>
    internal async ValueTask EnqueueShutdownMarkerAsync(List<Exception> failures, TimeSpan remaining)
    {
        // The marker wait is bounded by the shared budget: on a wedged thread with a full ring it
        // would otherwise hang disposal forever, before the join timeout below ever gets to report.
        using var markerCts = new CancellationTokenSource(remaining);
        try
        {
            await EnqueueShutdownAsync(markerCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            LogManager.JournalShutdownMarkerTimedOut(_logger);
            failures.Add(new TimeoutException("shutdown marker did not enter the journal ring within the shutdown budget."));
        }
    }

    internal void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        _owner.SetJournalThreadFailure(reason);
        FailPendingDurabilityAcks(reason);
        _owner.GroupCommit?.CancelPendingCore(reason);
    }

    internal void FailPendingDurabilityAcks(Exception reason)
    {
        var acks = _owner.DurabilityAcks.TakeAll(reason);

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

    /// <summary>Waits for the journal thread to exit within the given timeout without recording failures.</summary>
    /// <param name="timeout">Maximum time to wait for the thread exit.</param>
    /// <returns>Whether the journal thread exited in time.</returns>
    internal async ValueTask<bool> TryJoinJournalThreadAsync(TimeSpan timeout)
    {
        var work = new JoinJournalThreadWork(this, timeout);
        await WorkPool.RunAsync(work, TaskCreationOptions.LongRunning, CancellationToken.None).ConfigureAwait(false);
        return work.Joined;
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

    private void DetachDurabilityAck(TaskCompletionSource ack) => _ = _owner.DurabilityAcks.Remove(ack);

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _producerGate.Enter();
        try
        {
            _producerGate.ThrowIfShutdownInitiated();
            _owner.DurabilityAcks.Add(ack);
            try
            {
                var item = JournalWorkItem.DurabilityCheckpoint(ack);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // The item never entered the ring, so the journal thread will never resolve it:
                // detach here or the ack leaks into the registry until disposal.
                DetachDurabilityAck(ack);
                throw;
            }
        }
        finally
        {
            _producerGate.Exit();
        }

        // The durability wait stays outside the gate: the gate covers only the publish, so a slow
        // journal thread never blocks shutdown drain on fsync latency.
        try
        {
            await ack.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
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
        }
    }

    private ValueTask EnqueueShutdownAsync(CancellationToken cancellationToken) => _owner.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellationToken);

    private void RemoveDurabilityAck(TaskCompletionSource ack, CancellationToken cancellationToken)
    {
        if (!_owner.DurabilityAcks.Remove(ack))
            return;

        _ = ack.TrySetCanceled(cancellationToken);
    }

    private sealed class JoinJournalThreadWork : IWorkPoolItem
    {
        private readonly JournalDurabilityCoordinator _pipeline;
        private readonly TimeSpan _timeout;

        internal JoinJournalThreadWork(JournalDurabilityCoordinator pipeline, TimeSpan timeout)
        {
            _pipeline = pipeline;
            _timeout = timeout;
        }

        internal bool Joined { get; private set; }

        void IWorkPoolItem.Execute() => Joined = _pipeline._owner.JournalThread.Join(_timeout);
    }
}
