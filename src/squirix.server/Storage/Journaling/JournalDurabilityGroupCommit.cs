using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Batches journal durability flushes so concurrent mutations can share one fsync while each waiter
/// still observes durability before in-memory apply. Deadline evaluation runs on the journal thread.
/// </summary>
internal sealed class JournalDurabilityGroupCommit
{
    private readonly Action _journalThreadFlush;
    private readonly Action _notifyJournalThread;
    private readonly PersistenceOptions _opt;
    private readonly Lock _sync = new();
    private readonly BatchDeadline _batchDeadline = new();
    private readonly TimeProvider _timeProvider;

    private List<JournalDurabilityWaiter> _waiters;
    private List<JournalDurabilityWaiter> _waitersSpare;

    internal JournalDurabilityGroupCommit(Action journalThreadFlush, Action notifyJournalThread, PersistenceOptions opt, TimeProvider? timeProvider = null)
    {
        _journalThreadFlush = journalThreadFlush ?? throw new ArgumentNullException(nameof(journalThreadFlush));
        _notifyJournalThread = notifyJournalThread ?? throw new ArgumentNullException(nameof(notifyJournalThread));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _timeProvider = timeProvider ?? TimeProvider.System;

        var capacity = Math.Max(4, opt.JournalGroupCommitMaxBatch);
        _waiters = new List<JournalDurabilityWaiter>(capacity);
        _waitersSpare = new List<JournalDurabilityWaiter>(capacity);
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for the caller's batch.</returns>
    internal async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var waiter = JournalDurabilityWaiter.Rent();
        try
        {
            var waitTask = waiter.AwaitAsync(cancellationToken);
            var signalJournal = false;
            lock (_sync)
            {
                if (_waiters.Count is 0)
                {
                    _batchDeadline.Arm(_timeProvider.GetUtcNow().Add(_opt.JournalGroupCommitMaxWait).Ticks);
                    signalJournal = true;
                }

                _waiters.Add(waiter);
                if (_waiters.Count >= _opt.JournalGroupCommitMaxBatch)
                    signalJournal = true;
            }

            if (signalJournal)
                _notifyJournalThread();

            await waitTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (!CancelWaiter(waiter, cancellationToken))
                waiter.MarkAbandonedByCaller();

            throw;
        }
        finally
        {
            if (!waiter.IsAbandonedByCaller())
                waiter.ReturnToPool();
        }
    }

    /// <summary>Fails any pending commit waiters during shutdown or journal pipeline failure.</summary>
    /// <param name="reason">Failure reason propagated to pending waiters.</param>
    /// <returns>A completed task once pending waiters are failed.</returns>
    internal ValueTask CancelPendingAsync(Exception reason)
    {
        CancelPendingCore(reason);
        return ValueTask.CompletedTask;
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
            if (_waiters.Count is 0 || !_batchDeadline.IsArmed)
                return Timeout.Infinite;

            var remaining = TimeSpan.FromTicks(_batchDeadline.Ticks - _timeProvider.GetUtcNow().Ticks);
            if (remaining <= TimeSpan.Zero)
                return 0;

            return Convert.ToInt32(Math.Min(remaining.TotalMilliseconds, int.MaxValue));
        }
    }

    internal void CancelPendingCore(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (_sync)
        {
            _batchDeadline.Clear();
            for (var i = 0; i < _waiters.Count; i++)
                _waiters[i].SetException(reason);

            _waiters.Clear();
        }

        // ReturnToPool is owned by AwaitCommitAsync finally after the await completes.
    }

    private bool CancelWaiter(JournalDurabilityWaiter waiter, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_sync)
        {
            removed = _waiters.Remove(waiter);
            if (removed && _waiters.Count is 0)
                _batchDeadline.Clear();
        }

        if (removed)
            waiter.SetCanceled(cancellationToken);

        return removed;
    }

    private void CompleteBatchOnJournalThread(List<JournalDurabilityWaiter> batch)
    {
        try
        {
            // One journal-thread fsync covers every waiter captured in this due batch.
            _journalThreadFlush();
        }
        catch (IOException ex)
        {
            FailBatch(ex);
            return;
        }
        catch (ObjectDisposedException ex)
        {
            FailBatch(ex);
            return;
        }
        catch (InvalidOperationException ex)
        {
            FailBatch(ex);
            return;
        }

        for (var i = 0; i < batch.Count; i++)
        {
            var waiter = batch[i];

            // Callers that canceled before the flush still own returning their waiter to the pool.
            if (!waiter.IsAbandonedByCaller())
                waiter.SetResult();
        }

        for (var i = 0; i < batch.Count; i++)
        {
            if (batch[i].IsAbandonedByCaller())
                batch[i].ReturnToPool();
        }

        batch.Clear();
        return;

        void FailBatch(Exception ex)
        {
            // Flush failures fail the whole batch so no waiter observes partial durability.
            for (var i = 0; i < batch.Count; i++)
            {
                var waiter = batch[i];
                if (!waiter.IsAbandonedByCaller())
                    waiter.SetException(ex);
            }

            for (var i = 0; i < batch.Count; i++)
            {
                if (batch[i].IsAbandonedByCaller())
                    batch[i].ReturnToPool();
            }

            batch.Clear();
        }
    }

    private bool TryTakeDueBatch(out List<JournalDurabilityWaiter> batch)
    {
        lock (_sync)
        {
            if (_waiters.Count is 0)
            {
                batch = _waiters;
                return false;
            }

            var now = _timeProvider.GetUtcNow().Ticks;
            var due = _waiters.Count >= _opt.JournalGroupCommitMaxBatch || now >= _batchDeadline.Ticks;
            if (!due)
            {
                batch = _waiters;
                return false;
            }

            batch = _waiters;
            _waiters = _waitersSpare;
            _waitersSpare = batch;
            _batchDeadline.Clear();
            return true;
        }
    }

    /// <summary>Mutable group-commit batch deadline; keeps assignments off <see cref="JournalDurabilityGroupCommit" /> for ND1906.</summary>
    private sealed class BatchDeadline
    {
        internal bool IsArmed => Ticks is not 0;

        internal long Ticks { get; private set; }

        internal void Arm(long ticks) => Ticks = ticks;

        internal void Clear() => Ticks = 0;
    }
}
