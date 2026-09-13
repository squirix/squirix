using System;
using System.Collections.Generic;
using System.IO;
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
    private static readonly TimeSpan MaintenanceAbortBound = TimeSpan.FromSeconds(5);

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

    internal async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Fail fast on a dead pipeline with the same identity as the appended path. The registry
        // latch below (DurabilityAcks.Add) also bounds post-drain arrivals at the semaphore.
        ThrowIfJournalThreadFailed();
        _producerGate.Enter();
        try
        {
            _producerGate.ThrowIfShutdownInitiated();
            try
            {
                _owner.DurabilityAcks.Add(ack);
            }
            catch
            {
                ThrowIfJournalThreadFailed();
                throw;
            }

            try
            {
                await _owner.Ring.EnqueueAsync(JournalWorkItem.DurabilityCheckpoint(ack), cancellationToken, ThrowIfJournalThreadFailed).ConfigureAwait(false);
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

        // The durability wait stays outside the gate: the gate covers only the publication, so a slow
        // journal thread never blocks shutdown drain on fsync latency.
        try
        {
            await ack.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Removal winner owns the outcome: caller cancellation wins, else the drain fault.
            if (RemoveDurabilityAck(ack, cancellationToken))
                throw;

            // Drain won: propagate its result without the caller's token, not cancellation.
            await ack.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        finally
        {
            DetachDurabilityAck(ack);
        }
    }

    internal async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var publisher = new MaintenancePublisher(this);
        var begin = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await publisher.EnqueueItemAsync(begin, JournalWorkItem.MaintenanceBegin, cancellationToken).ConfigureAwait(false);
        await publisher.AwaitAckAsync(begin, cancellationToken).ConfigureAwait(false);

        // The End ack wait stays outside the try: once End is enqueued, the layout is consistent,
        // so cancelling the wait is harmless and must not poison the pipeline.
        // The catch below always rethrows, so reaching the wait proves End was enqueued.
        TaskCompletionSource end;
        try
        {
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _owner.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

            end = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            // Fail-fast after a drain reads as a Begin-without-End failure below: End never entered
            // the ring. The catch preserves the first failure.
            await publisher.EnqueueItemAsync(end, ack => JournalWorkItem.MaintenanceEnd(ack, resetSegmentIndex, resetSequence), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // "Begin" was processed but End never entered the ring: the segment path is released
            // while the in-memory counters are stale. Fail the pipeline loudly first (one drain
            // protocol, one latched error), then resync best-effort for observability. Rollback
            // is impossible in general (the disk is already mutated), so the failed pipeline
            // requires a restart.
            FailJournalPipeline(ex);
            _ = await TryPublishMaintenanceAbortAsync().ConfigureAwait(false);
            throw;
        }

        await publisher.AwaitAckAsync(end, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Enqueues the shutdown marker or fails disposal loudly when it cannot enter.</summary>
    /// <param name="failures">Disposal failures to record a marker timeout into.</param>
    /// <param name="cancellationToken">Budget for the marker wait; cancellation aborts disposal.</param>
    /// <returns>A task that completes when the marker entered the ring.</returns>
    internal async ValueTask EnqueueShutdownMarkerAsync(List<Exception> failures, CancellationToken cancellationToken)
    {
        // On a wedged thread with a full ring, the marker wait would otherwise hang disposal
        // forever, before the join timeout below ever gets to report.
        try
        {
            await EnqueueShutdownAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Without the marker, canceling or tearing down now would let the live thread exit
            // with queued frames unwritten. Fail reachable waiters explicitly and stop instead,
            // keeping the writer, ring, and gates alive.
            LogManager.JournalShutdownMarkerTimedOut(_logger);
            _owner.GroupCommit?.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator)));
            _ = _owner.PendingAppends.FailAll(new ObjectDisposedException(nameof(JournalCoordinator)), _logger, _owner.QueuedAppendsCounter);
            FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));
            failures.Add(new TimeoutException("shutdown marker did not enter the journal ring within the shutdown budget."));
            ThrowDisposeFailures(failures);
        }
    }

    internal void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);

        // The first failure wins, same as the Begin-without-End catch above: a fail-fast latch
        // reason must not replace the original error (e.g. a live thread faulting on I/O
        // after a maintenance action already failed the pipeline).
        _ = _owner.TrySetJournalThreadFailure(reason);

        // One-episode-one-error: concurrent callers share the coordinator-latched failure,
        // so every drain path reports the same instance instead of each caller's reason.
        var effective = _owner.GetJournalThreadFailure() ?? reason;
        _ = _owner.PendingAppends.FailAll(effective, _logger, _owner.QueuedAppendsCounter);

        FailPendingDurabilityAcks(effective);
        _owner.GroupCommit?.CancelPendingCore(effective);
    }

    internal void FailPendingDurabilityAcks(Exception reason)
    {
        var acks = _owner.DurabilityAcks.TakeAll(reason);

        for (var i = 0; i < acks.Count; i++)
            _ = acks[i].TrySetException(reason);

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

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

    /// <summary>Quiesces producers so the shutdown marker cannot overtake an admitted enqueue.</summary>
    /// <param name="failures">Disposal failures to record a quiescence timeout into.</param>
    /// <param name="remaining">Time left in the shared shutdown budget.</param>
    /// <returns>A task that completes when producers quiesced, or throws loudly when they did not.</returns>
    internal async ValueTask QuiesceProducersAsync(List<Exception> failures, TimeSpan remaining)
    {
        _producerGate.InitiateShutdown();
        if (await _producerGate.WaitAsync(remaining).ConfigureAwait(false))
            return;

        // Producers never quiesced: publishing the marker now could let it overtake an admitted
        // appending. Fail reachable waiters explicitly and stop instead of proceeding into
        // marker/join/teardown with a broken ordering guarantee.
        LogManager.JournalProducerQuiescenceTimedOut(_logger);
        _owner.GroupCommit?.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator)));
        _ = _owner.PendingAppends.FailAll(new ObjectDisposedException(nameof(JournalCoordinator)), _logger, _owner.QueuedAppendsCounter);
        FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));
        failures.Add(new TimeoutException("journal producers did not quiesce within the shutdown budget."));
        ThrowDisposeFailures(failures);
    }

    /// <summary>
    /// Drains appending admitted but never dequeued and returns quarantined buffers to the pool.
    /// Call only after the journal thread is joined: with a live thread the buffers must stay quarantined.
    /// </summary>
    internal void ReclaimAbandonedAppendsPostJoin()
    {
        _ = _owner.PendingAppends.FailAll(new ObjectDisposedException(nameof(JournalCoordinator)), _logger, _owner.QueuedAppendsCounter);
        _ = _owner.PendingAppends.ReturnQuarantinedBuffers();
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

    private static TimeSpan RemainingMaintenanceAbortTime(long deadline)
    {
        var remainingMs = deadline - Environment.TickCount64;
        return remainingMs <= 0 ? throw new TimeoutException("Maintenance abort exceeded its bound.") : TimeSpan.FromMilliseconds(remainingMs);
    }

    private void DetachDurabilityAck(TaskCompletionSource ack) => _ = _owner.DurabilityAcks.Remove(ack);

    private ValueTask EnqueueShutdownAsync(CancellationToken cancellationToken) => _owner.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellationToken);

    private bool RemoveDurabilityAck(TaskCompletionSource ack, CancellationToken cancellationToken)
    {
        if (!_owner.DurabilityAcks.Remove(ack))
            return false;

        _ = ack.TrySetCanceled(cancellationToken);
        return true;
    }

    /// <summary>Best-effort maintenance abort publish for observability on a failed pipeline.</summary>
    /// <returns>Whether the abort was published and acked before the bound expired.</returns>
    private async ValueTask<bool> TryPublishMaintenanceAbortAsync()
    {
        // During shutdown teardown already fails waiters loudly; an abort there is pointless work.
        // This check is only an optimization: the ring is FIFO, so either Abort/marker order is safe.
        if (_producerGate.IsShutdownInitiated)
            return false;

        // Enter without ThrowIfShutdownInitiated: Enter is a plain in-flight counter, so the
        // quiescence accounting stays intact and the shutdown marker cannot overtake this publication.
        var deadline = Environment.TickCount64 + Convert.ToInt64(MaintenanceAbortBound.TotalMilliseconds);
        _producerGate.Enter();
        try
        {
            var abortAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _owner.PendingAppends.TrackAbort(abortAck);
            try
            {
                using var enqueueCts = new CancellationTokenSource(RemainingMaintenanceAbortTime(deadline));

                // No failure check here by design: the abort publishes on an already-failed pipeline
                // and is bounded by its own enqueue/ack budgets instead.
                await _owner.Ring.EnqueueAsync(JournalWorkItem.MaintenanceAbort(abortAck), enqueueCts.Token).ConfigureAwait(false);
                using var ackCts = new CancellationTokenSource(RemainingMaintenanceAbortTime(deadline));
                await abortAck.Task.WaitAsync(ackCts.Token).ConfigureAwait(false);
                return true;
            }
            finally
            {
                _ = _owner.PendingAppends.RemoveAbort(abortAck);
            }
        }
        catch (Exception abortEx) when (abortEx is OperationCanceledException or TimeoutException or ObjectDisposedException or IOException or UnauthorizedAccessException
                                            or ArgumentException)
        {
            // The abort is diagnostics-only on an already-failed pipeline: log it as suppressed.
            // The pipeline failure slot was already poisoned by the caller with the original error.
            LogManager.MaintenanceAbortFailed(_logger, abortEx);
            return false;
        }
        catch (Exception unexpectedEx) when (unexpectedEx is not (OperationCanceledException or TimeoutException or ObjectDisposedException or IOException
                                                 or UnauthorizedAccessException or ArgumentException))
        {
            // Total guard: anything outside the expected set (framework bugs, fatal runtime errors)
            // must still never replace the original error awaiting the caller. The caller
            // already poisoned the pipeline failure slot, so logging here preserves the loud failure.
            LogManager.MaintenanceAbortFailed(_logger, unexpectedEx);
            return false;
        }
        finally
        {
            _producerGate.Exit();
        }
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

    /// <summary>Tracked maintenance publish and wait for the durability coordinator.</summary>
    private sealed class MaintenancePublisher
    {
        private readonly JournalDurabilityCoordinator _pipeline;

        internal MaintenancePublisher(JournalDurabilityCoordinator pipeline)
        {
            _pipeline = pipeline;
        }

        internal async ValueTask AwaitAckAsync(TaskCompletionSource ack, CancellationToken cancellationToken)
        {
            try
            {
                await ack.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Whoever wins the removal owns the outcome: the caller reports cancellation,
                // otherwise the drain already faulted the waiter.
                if (_pipeline._owner.PendingAppends.RemoveMaintenance(ack))
                {
                    _ = ack.TrySetCanceled(cancellationToken);
                    throw;
                }

                // The drain won the removal, so it owns the ack outcome: propagate its result
                // without the caller's cancellation token instead of masking it as cancellation.
                await ack.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                throw;
            }
            finally
            {
                _ = _pipeline._owner.PendingAppends.RemoveMaintenance(ack);
            }
        }

        internal async ValueTask EnqueueItemAsync(TaskCompletionSource ack, Func<TaskCompletionSource, JournalWorkItem> createItem, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(ack);
            ArgumentNullException.ThrowIfNull(createItem);
            try
            {
                _pipeline._owner.PendingAppends.TrackMaintenance(ack);
            }
            catch
            {
                _pipeline.ThrowIfJournalThreadFailed();
                throw;
            }

            _pipeline._producerGate.Enter();
            try
            {
                _pipeline._producerGate.ThrowIfShutdownInitiated();
                await _pipeline._owner.Ring.EnqueueAsync(createItem(ack), cancellationToken, _pipeline.ThrowIfJournalThreadFailed).ConfigureAwait(false);
            }
            catch
            {
                // The item never entered the ring, so the journal thread will never resolve it:
                // detach here or the ack leaks into the registry until disposal.
                _ = _pipeline._owner.PendingAppends.RemoveMaintenance(ack);
                throw;
            }
            finally
            {
                _pipeline._producerGate.Exit();
            }
        }
    }
}
