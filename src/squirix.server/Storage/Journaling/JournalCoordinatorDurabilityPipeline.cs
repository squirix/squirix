using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Durability, maintenance, and failure handling for <see cref="JournalCoordinator" />.</summary>
internal sealed class JournalCoordinatorDurabilityPipeline
{
    private readonly JournalCoordinator _owner;

    internal JournalCoordinatorDurabilityPipeline(JournalCoordinator owner)
    {
        _owner = owner;
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
            if (!await Task.Factory.StartNew(
                    () => _owner.JournalThread.Join(TimeSpan.FromSeconds(30)),
                    _owner.BackgroundCancellation.Token,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default).ConfigureAwait(false))
            {
                failures.Add(new TimeoutException("journal I/O thread did not exit within 30 seconds."));
            }
        }
        catch (OperationCanceledException) when (_owner.BackgroundCancellation.IsCancellationRequested)
        {
            // Dispose Canceled the join wait when teardown already completed.
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
    }

    internal void CompleteDurabilityCheckpointOnJournalThread()
    {
        if (!_owner.DurabilityWaiters.TryTakeAllIfAny(out var waiters))
        {
            _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
            return;
        }

        _owner.EventLoop.FsyncOnJournalThread();

        for (var i = 0; i < waiters.Count; i++)
            _ = waiters[i].TrySetResult();

        _ = Interlocked.Exchange(ref _owner.DurabilityFlushScheduledFlag.Value, 0);
    }

    internal async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = JournalDurabilityWaiter.Rent();
        try
        {
            var beginWaitTask = begin.AwaitAsync(cancellationToken);
            var beginItem = new JournalWorkItem(JournalWorkKind.MaintenanceBegin, completion: begin);
            await _owner.Ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);

            await beginWaitTask.ConfigureAwait(false);
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _owner.ManifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _owner.Options);

            var end = JournalDurabilityWaiter.Rent();
            try
            {
                var endWaitTask = end.AwaitAsync(cancellationToken);
                var endItem = new JournalWorkItem(
                    JournalWorkKind.MaintenanceEnd,
                    completion: end,
                    resetSegmentIndex: resetSegmentIndex,
                    resetSequence: resetSequence);
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

    internal ValueTask EnqueueShutdownAsync()
    {
        var shutdownItem = new JournalWorkItem(JournalWorkKind.Shutdown);
        return _owner.Ring.EnqueueAsync(shutdownItem, CancellationToken.None);
    }

    internal void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        Volatile.Write(ref _owner.JournalThreadFailureField, reason);
        FailPendingDurabilityWaiters(reason);
        _owner.GroupCommit?.CancelPendingCore(reason);
    }

    internal void FailPendingDurabilityWaiters(Exception reason)
    {
        var waiters = _owner.DurabilityWaiters.TakeAll();

        for (var i = 0; i < waiters.Count; i++)
            _ = waiters[i].TrySetException(reason);

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
        _owner.EventLoop.MarkRollCompletionPending();
        _owner.Ring.NotifyWorkAvailable();
    }

    internal void ThrowIfJournalThreadFailed()
    {
        if (Volatile.Read(ref _owner.JournalThreadFailureField) is { } failure)
            throw new InvalidOperationException("journal I/O thread failed.", failure);
    }

    internal async ValueTask WaitForSnapshotCutAdmissionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await _owner.WaitForPendingMemoryApplyDrainAsync(cancellationToken).ConfigureAwait(false);
            await _owner.MutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!_owner.HasPendingMemoryApply())
                return;

            _ = _owner.MutationGate.Release();
        }
    }

    private void DetachDurabilityWaiter(JournalDurabilityWaiter waiter) => _ = _owner.DurabilityWaiters.Remove(waiter);

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var waiter = JournalDurabilityWaiter.Rent();
        _owner.DurabilityWaiters.Add(waiter);

        try
        {
            var waitTask = waiter.AwaitAsync(cancellationToken);
            var item = new JournalWorkItem(JournalWorkKind.DurabilityCheckpoint);
            await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);

            await waitTask.ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            RemoveDurabilityWaiter(waiter, cancellationToken);
            throw;
        }
        finally
        {
            DetachDurabilityWaiter(waiter);
            waiter.ReturnToPool();
        }
    }

    private void RemoveDurabilityWaiter(JournalDurabilityWaiter waiter, CancellationToken cancellationToken)
    {
        if (!_owner.DurabilityWaiters.Remove(waiter))
            return;

        _ = waiter.TrySetCanceled(cancellationToken);
    }
}
