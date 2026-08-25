using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Batches journal durability flushes so concurrent mutations can share one fsync while each ack
/// still observes durability before in-memory apply. Deadline evaluation runs on the journal thread.
/// </summary>
internal sealed class JournalDurabilityGroupCommit
{
    private readonly BatchDeadline _batchDeadline = new();
    private readonly Action _journalThreadFlush;
    private readonly Action _notifyJournalThread;
    private readonly PersistenceOptions _opt;
    private readonly Lock _sync = new();
    private readonly TimeProvider _timeProvider;

    private List<TaskCompletionSource> _waits;
    private List<TaskCompletionSource> _waitsSpare;
    private Exception? _failure;

    internal JournalDurabilityGroupCommit(Action journalThreadFlush, Action notifyJournalThread, PersistenceOptions opt, TimeProvider? timeProvider = null)
    {
        _journalThreadFlush = journalThreadFlush ?? throw new ArgumentNullException(nameof(journalThreadFlush));
        _notifyJournalThread = notifyJournalThread ?? throw new ArgumentNullException(nameof(notifyJournalThread));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _timeProvider = timeProvider ?? TimeProvider.System;

        var capacity = Math.Max(4, opt.JournalGroupCommitMaxBatch);
        _waits = new List<TaskCompletionSource>(capacity);
        _waitsSpare = new List<TaskCompletionSource>(capacity);
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for the caller's batch.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the journal pipeline has already failed.</exception>
    internal async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var wait = DurabilityAckRegistry.NewWait();
        try
        {
            var waitTask = wait.Task.WaitAsync(cancellationToken);
            var signalJournal = false;
            lock (_sync)
            {
                // The latch check is atomic against CancelPendingCore's sweep: registering after the
                // journal thread died would otherwise park this waiter until dispose.
                if (_failure is { } failure)
                    throw new InvalidOperationException("journal I/O thread failed.", failure);

                if (_waits.Count == 0)
                {
                    _batchDeadline.Arm(_timeProvider.GetUtcNow().Add(_opt.JournalGroupCommitMaxWait).Ticks);
                    signalJournal = true;
                }

                _waits.Add(wait);
                if (_waits.Count >= _opt.JournalGroupCommitMaxBatch)
                    signalJournal = true;
            }

            if (signalJournal)
                _notifyJournalThread();

            await waitTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A wait already taken by the journal thread stays in the batch; the journal thread then
            // completes the orphaned source harmlessly (Try-set on a source nobody observes).
            lock (_sync)
            {
                if (_waits.Remove(wait) && _waits.Count == 0)
                    _batchDeadline.Clear();
            }

            throw;
        }
    }

    /// <summary>Fails any pending commit waits during shutdown or journal pipeline failure.</summary>
    /// <param name="reason">Failure reason propagated to pending waits.</param>
    internal void CancelPending(Exception reason) => CancelPendingCore(reason);

    internal void CancelPendingCore(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (_sync)
        {
            _failure ??= reason;
            _batchDeadline.Clear();
            for (var i = 0; i < _waits.Count; i++)
                _ = _waits[i].TrySetException(reason);

            _waits.Clear();
        }
    }

    /// <summary>Drains due batches on the journal thread.</summary>
    internal void DrainDueBatchesOnJournalThread()
    {
        while (TryTakeDueBatch(out var batch))
            CompleteBatchOnJournalThread(batch);
    }

    /// <summary>Milliseconds until the active batch deadline, or <see cref="Timeout.Infinite" /> when idle.</summary>
    /// <returns>Wait timeout in milliseconds for the journal thread idle loop.</returns>
    internal int GetJournalThreadWaitTimeoutMs()
    {
        lock (_sync)
        {
            if (_waits.Count == 0 || !_batchDeadline.IsArmed)
                return Timeout.Infinite;

            var remaining = TimeSpan.FromTicks(_batchDeadline.Ticks - _timeProvider.GetUtcNow().Ticks);
            if (remaining <= TimeSpan.Zero)
                return 0;

            return Convert.ToInt32(Math.Min(remaining.TotalMilliseconds, int.MaxValue));
        }
    }

    private static void CompleteBatchWithFailure(List<TaskCompletionSource> batch, Exception ex)
    {
        // Flush failures fail the whole batch so no wait observes partial durability.
        for (var i = 0; i < batch.Count; i++)
            _ = batch[i].TrySetException(ex);

        batch.Clear();
    }

    private static void CompleteBatchWithSuccess(List<TaskCompletionSource> batch)
    {
        for (var i = 0; i < batch.Count; i++)
            _ = batch[i].TrySetResult();

        batch.Clear();
    }

    private void CompleteBatchOnJournalThread(List<TaskCompletionSource> batch)
    {
        try
        {
            // One journal-thread fsync covers every ack captured in this due batch.
            _journalThreadFlush();
        }
        catch (Exception ex)
        {
            CompleteBatchWithFailure(batch, ex);
            if (ex is not (IOException or ObjectDisposedException or InvalidOperationException))
                throw;

            return;
        }

        CompleteBatchWithSuccess(batch);
    }

    private bool TryTakeDueBatch(out List<TaskCompletionSource> batch)
    {
        lock (_sync)
        {
            if (_waits.Count == 0)
            {
                batch = _waits;
                return false;
            }

            var now = _timeProvider.GetUtcNow().Ticks;
            var due = _waits.Count >= _opt.JournalGroupCommitMaxBatch || now >= _batchDeadline.Ticks;
            if (!due)
            {
                batch = _waits;
                return false;
            }

            batch = _waits;
            _waits = _waitsSpare;
            _waitsSpare = batch;
            _batchDeadline.Clear();
            return true;
        }
    }

    /// <summary>Mutable group-commit batch deadline; keeps assignments off <see cref="JournalDurabilityGroupCommit" /> for ND1906.</summary>
    private sealed class BatchDeadline
    {
        internal bool IsArmed => Ticks != 0;

        internal long Ticks { get; private set; }

        internal void Arm(long ticks) => Ticks = ticks;

        internal void Clear() => Ticks = 0;
    }
}
