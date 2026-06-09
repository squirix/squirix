using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Batches journal durability flushes so concurrent mutations can share one fsync while each waiter
/// still observes durability before in-memory apply.
/// </summary>
internal sealed class JournalDurabilityGroupCommit
{
    private readonly Func<CancellationToken, ValueTask> _flushAsync;
    private readonly PersistenceOptions _opt;
    private readonly Lock _sync = new();
    private CancellationTokenSource? _delayCts;
    private int _drainGate;
    private List<TaskCompletionSource> _waiters = [];

    public JournalDurabilityGroupCommit(Func<CancellationToken, ValueTask> flushAsync, PersistenceOptions opt)
    {
        _flushAsync = flushAsync ?? throw new ArgumentNullException(nameof(flushAsync));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
    }

    /// <summary>
    /// Waits until appended journal bytes through the caller's append are covered by a durability flush.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the caller's append is durable.</returns>
    public async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scheduleDelay = false;
        var flushImmediately = false;

        lock (_sync)
        {
            _waiters.Add(waiter);
            if (_waiters.Count == 1)
                scheduleDelay = true;
            else if (_waiters.Count >= _opt.JournalGroupCommitMaxBatch)
                flushImmediately = true;
        }

        if (flushImmediately)
            CancelDelayTimer();
        else if (scheduleDelay)
            _ = ScheduleDelayFlushAsync();

        if (flushImmediately)
            await DrainPendingCommitsAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await waiter.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            CancelWaiter(waiter, cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Fails any pending commit waiters during shutdown.
    /// </summary>
    /// <param name="reason">The exception propagated to pending waiters.</param>
    public void CancelPending(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        List<TaskCompletionSource> pending;
        lock (_sync)
        {
            CancelDelayTimerLocked();
            pending = _waiters;
            _waiters = [];
        }

        foreach (var waiter in pending)
            _ = waiter.TrySetException(reason);
    }

    private void CancelDelayTimer()
    {
        lock (_sync)
            CancelDelayTimerLocked();
    }

    private void CancelDelayTimerLocked()
    {
        var cts = _delayCts;
        _delayCts = null;
        if (cts is null)
            return;

        try
        {
            cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Delay timer already torn down during shutdown.
        }

        cts.Dispose();
    }

    private void CancelWaiter(TaskCompletionSource waiter, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_sync)
        {
            removed = _waiters.Remove(waiter);
            var cancelDelay = removed && _waiters.Count == 0;
            if (cancelDelay)
                CancelDelayTimerLocked();
        }

        if (removed)
            _ = waiter.TrySetCanceled(cancellationToken);
    }

    private async Task DrainPendingCommitsAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _drainGate, 1, 0) != 0)
            return;

        try
        {
            while (true)
            {
                List<TaskCompletionSource> batch;
                lock (_sync)
                {
                    if (_waiters.Count == 0)
                        return;

                    batch = _waiters;
                    _waiters = [];
                    CancelDelayTimerLocked();
                }

                try
                {
                    await _flushAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (IOException ex)
                {
                    foreach (var waiter in batch)
                        _ = waiter.TrySetException(ex);

                    throw;
                }
                catch (ObjectDisposedException ex)
                {
                    foreach (var waiter in batch)
                        _ = waiter.TrySetException(ex);

                    throw;
                }
                catch (InvalidOperationException ex)
                {
                    foreach (var waiter in batch)
                        _ = waiter.TrySetException(ex);

                    throw;
                }

                foreach (var waiter in batch)
                    _ = waiter.TrySetResult();
            }
        }
        finally
        {
            _ = Interlocked.Exchange(ref _drainGate, 0);
            bool hasMoreWaiters;
            lock (_sync)
                hasMoreWaiters = _waiters.Count > 0;

            if (hasMoreWaiters)
                await DrainPendingCommitsAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ScheduleDelayFlushAsync()
    {
        var delayCts = new CancellationTokenSource();
        lock (_sync)
        {
            CancelDelayTimerLocked();
            _delayCts = delayCts;
        }

        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_opt.JournalGroupCommitMaxWaitMs), delayCts.Token).ConfigureAwait(false);
            await DrainPendingCommitsAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (delayCts.IsCancellationRequested)
        {
            // Superseded by an immediate batch flush or shutdown cancellation of the delay timer.
        }
        catch (IOException ex)
        {
            CancelPending(ex);
        }
        catch (ObjectDisposedException ex)
        {
            CancelPending(ex);
        }
        catch (InvalidOperationException ex)
        {
            CancelPending(ex);
        }
    }
}
