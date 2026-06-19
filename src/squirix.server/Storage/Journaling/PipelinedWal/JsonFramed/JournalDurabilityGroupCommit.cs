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
    private readonly TimeProvider _timeProvider;
    private CancellationTokenSource? _delayCts;
    private int _drainGate;
    private List<TaskCompletionSource> _waiters = [];

    public JournalDurabilityGroupCommit(Func<CancellationToken, ValueTask> flushAsync, PersistenceOptions opt, TimeProvider? timeProvider = null)
    {
        _flushAsync = flushAsync ?? throw new ArgumentNullException(nameof(flushAsync));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
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
            if (_waiters.Count is 1)
                scheduleDelay = true;
            else if (_waiters.Count >= _opt.JournalGroupCommitMaxBatch)
                flushImmediately = true;
        }

        if (flushImmediately)
            await CancelDelayTimerAsync().ConfigureAwait(false);
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
            await CancelWaiterAsync(waiter, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Fails any pending commit waiters during shutdown.</summary>
    /// <param name="reason">The exception propagated to pending waiters.</param>
    /// <returns>A <see cref="ValueTask" /> that completes after pending waiters are failed.</returns>
    public async ValueTask CancelPendingAsync(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        List<TaskCompletionSource> pending;
        CancellationTokenSource? delayCts;
        lock (_sync)
        {
            delayCts = TakeDelayCtsLocked();
            pending = _waiters;
            _waiters = [];
        }

        await CancelAndDisposeDelayCtsAsync(delayCts).ConfigureAwait(false);

        foreach (var waiter in pending)
            _ = waiter.TrySetException(reason);
    }

    private static async ValueTask CancelAndDisposeDelayCtsAsync(CancellationTokenSource? cts)
    {
        if (cts is null)
            return;

        try
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Delay timer already torn down during shutdown.
        }

        cts.Dispose();
    }

    private static void FailBatchWaiters(List<TaskCompletionSource> batch, Exception ex)
    {
        foreach (var waiter in batch)
            _ = waiter.TrySetException(ex);
    }

    private CancellationTokenSource? TakeDelayCtsLocked()
    {
        var cts = _delayCts;
        _delayCts = null;
        return cts;
    }

    private async ValueTask CancelDelayTimerAsync()
    {
        CancellationTokenSource? delayCts;
        lock (_sync)
            delayCts = TakeDelayCtsLocked();

        await CancelAndDisposeDelayCtsAsync(delayCts).ConfigureAwait(false);
    }

    private async ValueTask CancelWaiterAsync(TaskCompletionSource waiter, CancellationToken cancellationToken)
    {
        bool removed;
        CancellationTokenSource? delayCts = null;
        lock (_sync)
        {
            removed = _waiters.Remove(waiter);
            if (removed && _waiters.Count is 0)
                delayCts = TakeDelayCtsLocked();
        }

        await CancelAndDisposeDelayCtsAsync(delayCts).ConfigureAwait(false);

        if (removed)
            _ = waiter.TrySetCanceled(cancellationToken);
    }

    private async Task DrainPendingCommitsAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _drainGate, 1, 0) is not 0)
            return;

        try
        {
            while (true)
            {
                List<TaskCompletionSource> batch;
                CancellationTokenSource? delayCts;
                lock (_sync)
                {
                    if (_waiters.Count is 0)
                        return;

                    batch = _waiters;
                    _waiters = [];
                    delayCts = TakeDelayCtsLocked();
                }

                await CancelAndDisposeDelayCtsAsync(delayCts).ConfigureAwait(false);
                await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
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

    private async Task FlushBatchAsync(List<TaskCompletionSource> batch, CancellationToken cancellationToken)
    {
        try
        {
            await _flushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (IOException ex)
        {
            FailBatchWaiters(batch, ex);
            throw;
        }
        catch (ObjectDisposedException ex)
        {
            FailBatchWaiters(batch, ex);
            throw;
        }
        catch (InvalidOperationException ex)
        {
            FailBatchWaiters(batch, ex);
            throw;
        }

        foreach (var waiter in batch)
            _ = waiter.TrySetResult();
    }

    private async Task ScheduleDelayFlushAsync()
    {
        var delayCts = new CancellationTokenSource();
        CancellationTokenSource? previous;
        lock (_sync)
        {
            previous = TakeDelayCtsLocked();
            _delayCts = delayCts;
        }

        await CancelAndDisposeDelayCtsAsync(previous).ConfigureAwait(false);

        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_opt.JournalGroupCommitMaxWaitMs), _timeProvider, delayCts.Token).ConfigureAwait(false);
            await DrainPendingCommitsAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (delayCts.IsCancellationRequested)
        {
            // Superseded by an immediate batch flush or shutdown cancellation of the delay timer.
        }
        catch (IOException ex)
        {
            await CancelPendingAsync(ex).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            await CancelPendingAsync(ex).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            await CancelPendingAsync(ex).ConfigureAwait(false);
        }
    }
}
