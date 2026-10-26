using System;
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
        ThrowIfJournalThreadFailedCheck = ThrowIfJournalThreadFailed;
    }

    internal Action ThrowIfJournalThreadFailedCheck { get; }

    /// <summary>Waits for the ack of a checkpoint published by <see cref="PublishFlushAsync" />.</summary>
    /// <param name="published">Ack returned by <see cref="PublishFlushAsync" />, possibly still being published.</param>
    /// <param name="cancellationToken">Cancels the wait; the checkpoint itself stays on the ring.</param>
    /// <returns>A task that completes once every frame enqueued before the checkpoint is durable.</returns>
    /// <remarks>Taking the publication itself lets a caller that publishes and waits in one step use a single state machine.</remarks>
    internal async ValueTask AwaitFlushAsync(ValueTask<TaskCompletionSource> published, CancellationToken cancellationToken)
    {
        var ack = await published.ConfigureAwait(false);

        // The durability wait stays outside the gate: the gate covers only the publication, so a slow
        // journal thread never blocks shutdown drain on fsync latency.
        try
        {
            await ack.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _owner.StallProbe.ReportWaitCanceled("durability commit");

            // Removal winner owns the outcome: caller cancellation wins, else the drain fault.
            if (_owner.DurabilityAcks.Remove(ack))
            {
                _ = ack.TrySetCanceled(cancellationToken);
                throw;
            }

            // Drain won: propagate its result without the caller's token, not cancellation.
            await ack.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        finally
        {
            DetachDurabilityAck(ack);
        }
    }

    internal void CompleteCheckpointOnJournalThread(JournalWorkItem item)
    {
        var ack = ThrowHelper.Required(item.Ack, "durability checkpoint work item is missing a durability ack.");

        // Complete only the ack carried by this work item. The ack rides the ring position of its
        // own checkpoint, so a flush performed here is guaranteed to cover every frame enqueued before
        // it. Completing acks registered later (their checkpoints are still queued behind this item)
        // would report frames durable before they are written, so foreign acks must stay pending.
        // Marking the ack in flight keeps it reachable for the failure and shutdown drains while the
        // fsync runs, but a caller cancel can no longer win it: Remove only takes pending acks. When the
        // fsync fails the pipeline is latched first, then the ack is faulted, then the failure is rethrown; the repeated fail in
        // the loop is harmless, and the fsync is never retried. One failure per episode still holds per caller: a caller may observe the
        // shutdown fault while the latch later records the I/O error of the same stuck fsync, but no
        // caller observes both.
        if (_owner.DurabilityAcks.TryMarkInFlight(ack))
        {
            try
            {
                _owner.EventLoop.FlushToDisk();
                _ = ack.TrySetResult();
            }
            catch (Exception ex)
            {
                // Latched before the ack faults, so a caller that observes the fault already sees the failed pipeline.
                FailJournalPipeline(ex);
                _ = ack.TrySetException(ex);
                throw;
            }
            finally
            {
                _owner.DurabilityAcks.Complete(ack);
            }
        }

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    /// <summary>Stops tracking a checkpoint ack; a checkpoint already on the ring still runs, but nobody observes its outcome.</summary>
    /// <param name="ack">Ack returned by <see cref="PublishFlushAsync" />.</param>
    internal void DetachDurabilityAck(TaskCompletionSource ack) => _ = _owner.DurabilityAcks.Remove(ack);

    internal ValueTask EnqueueFlushAsync(CancellationToken cancellationToken) => AwaitFlushAsync(PublishFlushAsync(cancellationToken), cancellationToken);

    /// <summary>Enqueues the shutdown marker unless the journal thread already exited.</summary>
    /// <param name="wait">Time the marker may wait for a ring slot.</param>
    /// <returns>
    /// <see langword="true" /> when the marker entered the ring or the journal thread already exited, so nothing is left to enqueue it for;
    /// <see langword="false" /> when the wait ran out and no marker is on the ring.
    /// </returns>
    internal async ValueTask<bool> EnqueueShutdownMarkerAsync(TimeSpan wait)
    {
        // A journal thread that already exited (a latched failure ends it) never dequeues the marker, so a full ring would otherwise
        // hold the marker for the whole wait.
        if (!_owner.JournalThread.IsAlive)
            return true;

        // On a wedged thread with a full ring, the marker wait would otherwise hang the stop forever.
        using var markerCts = new CancellationTokenSource(wait);
        try
        {
            await _owner.Ring.EnqueueAsync(JournalWorkItem.Shutdown(), markerCts.Token, ThrowIfJournalThreadExited).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (markerCts.IsCancellationRequested)
        {
            return false;
        }
        catch (InvalidOperationException) when (!_owner.JournalThread.IsAlive)
        {
            // The thread exited while the marker waited for a slot: the ring took no marker.
            return true;
        }
    }

    internal void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);

        // The first failure wins: a fail-fast latch reason must not replace the original error
        // (e.g. a live thread faulting on I/O after another path already failed the pipeline).
        _ = _owner.TrySetJournalThreadFailure(reason);

        // One-episode-one-error: concurrent callers share the coordinator-latched failure,
        // so every drain path reports the same instance instead of each caller's reason.
        var effective = _owner.GetJournalThreadFailure() ?? reason;
        _ = _owner.PendingAppends.FailAll(effective, _logger, _owner.QueuedAppendsCounter);

        _ = FailPendingDurabilityAcks(effective);
        _ = _owner.GroupCommit?.CancelPendingCore(effective);
    }

    /// <summary>Faults every tracked durability checkpoint, pending or in flight, and closes the registry.</summary>
    /// <param name="reason">Failure reason propagated to the checkpoints and late arrivals.</param>
    /// <returns>Number of drained checkpoints whose fsync was in flight.</returns>
    internal int FailPendingDurabilityAcks(Exception reason)
    {
        var acks = _owner.DurabilityAcks.TakeAll(reason, out var inFlightCount);

        acks.FaultAll(reason);

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
        return inFlightCount;
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

    /// <summary>Publishes a durability checkpoint behind every frame already on the ring, without waiting for it.</summary>
    /// <param name="cancellationToken">Cancels ring admission.</param>
    /// <returns>The checkpoint ack to pass to <see cref="AwaitFlushAsync" /> or <see cref="DetachDurabilityAck" />.</returns>
    internal async ValueTask<TaskCompletionSource> PublishFlushAsync(CancellationToken cancellationToken)
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
                await _owner.Ring.EnqueueAsync(JournalWorkItem.DurabilityCheckpoint(ack), cancellationToken, ThrowIfJournalThreadFailedCheck).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Parked at a full ring: a stalled journal thread is not draining it.
                DetachDurabilityAck(ack);
                _owner.StallProbe.ReportWaitCanceled("journal ring admission");
                throw;
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

        return ack;
    }

    /// <summary>Quiesces producers so the shutdown marker cannot overtake an admitted enqueue.</summary>
    /// <param name="wait">Time producers may take to publish their in-flight enqueues.</param>
    /// <returns>Whether every in-flight enqueue was published in time; new work is refused from here on either way.</returns>
    internal ValueTask<bool> QuiesceProducersAsync(TimeSpan wait)
    {
        _producerGate.InitiateShutdown();
        return _producerGate.WaitAsync(wait);
    }

    /// <summary>
    /// Drains appending admitted but never dequeued and returns quarantined buffers to the pool.
    /// Call only after the journal thread is joined: with a live thread the buffers must stay quarantined.
    /// </summary>
    /// <returns>The number of admitted appends that were never written; an upper bound, since an append admitted but refused before the ring is counted too.</returns>
    internal int ReclaimAbandonedAppendsPostJoin()
    {
        var abandoned = _owner.PendingAppends.FailAll(new JournalShutdownRefusedException(nameof(JournalCoordinator)), _logger, _owner.QueuedAppendsCounter);
        _ = _owner.PendingAppends.ReturnQuarantinedBuffers();
        return abandoned;
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

    /// <summary>Stops admitting grouped writers, waits for the admitted ones to finish their apply, and takes the mutation gate for the cut.</summary>
    /// <param name="cancellationToken">Cancels the wait; admission reopens on cancellation or failure.</param>
    /// <returns>The mutation gate holder, with admission already reopened.</returns>
    /// <remarks>
    /// Without the closed admission, writers that keep entering would hold the in-flight count above zero for ever. Admission reopens only after the gate is
    /// held: a writer enters only under the gate, so none can slip in between the reopening and the capture.
    /// </remarks>
    internal async ValueTask<AsyncLockHolder> WaitForSnapshotCutAdmissionAsync(CancellationToken cancellationToken)
    {
        var admission = _snapshot.InFlightApplyGate;
        admission.Close();
        try
        {
            while (true)
            {
                await admission.WaitAsync(cancellationToken).ConfigureAwait(false);
                var gateGuard = await _snapshot.MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
                if (!admission.HasPending)
                {
                    admission.Open();
                    return gateGuard;
                }

                gateGuard.Dispose();
            }
        }
        catch
        {
            admission.Open();
            throw;
        }
    }

    private void ThrowIfJournalThreadExited()
    {
        if (!_owner.JournalThread.IsAlive)
            throw new InvalidOperationException("journal I/O thread exited.");
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
