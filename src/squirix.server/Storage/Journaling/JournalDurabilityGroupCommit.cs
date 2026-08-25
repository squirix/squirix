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

    private List<DurabilityAck> _acks;
    private List<DurabilityAck> _acksSpare;

    internal JournalDurabilityGroupCommit(Action journalThreadFlush, Action notifyJournalThread, PersistenceOptions opt, TimeProvider? timeProvider = null)
    {
        _journalThreadFlush = journalThreadFlush ?? throw new ArgumentNullException(nameof(journalThreadFlush));
        _notifyJournalThread = notifyJournalThread ?? throw new ArgumentNullException(nameof(notifyJournalThread));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _timeProvider = timeProvider ?? TimeProvider.System;

        var capacity = Math.Max(4, opt.JournalGroupCommitMaxBatch);
        _acks = new List<DurabilityAck>(capacity);
        _acksSpare = new List<DurabilityAck>(capacity);
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for the caller's batch.</returns>
    internal async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var ack = DurabilityAck.Rent();
        try
        {
            var ackWaitTask = ack.AwaitAsync(cancellationToken);
            var signalJournal = false;
            lock (_sync)
            {
                if (_acks.Count == 0)
                {
                    _batchDeadline.Arm(_timeProvider.GetUtcNow().Add(_opt.JournalGroupCommitMaxWait).Ticks);
                    signalJournal = true;
                }

                _acks.Add(ack);
                if (_acks.Count >= _opt.JournalGroupCommitMaxBatch)
                    signalJournal = true;
            }

            if (signalJournal)
                _notifyJournalThread();

            await ackWaitTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (!CancelAck(ack, cancellationToken))
                ack.MarkAbandonedByCaller();

            throw;
        }
        finally
        {
            if (!ack.IsAbandonedByCaller())
                ack.ReturnToPool();
        }
    }

    /// <summary>Fails any pending commit acks during shutdown or journal pipeline failure.</summary>
    /// <param name="reason">Failure reason propagated to pending acks.</param>
    internal void CancelPending(Exception reason) => CancelPendingCore(reason);

    internal void CancelPendingCore(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (_sync)
        {
            _batchDeadline.Clear();
            for (var i = 0; i < _acks.Count; i++)
                _acks[i].SetException(reason);

            _acks.Clear();
        }

        // ReturnToPool is owned by AwaitCommitAsync finally after the await completes.
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
            if (_acks.Count == 0 || !_batchDeadline.IsArmed)
                return Timeout.Infinite;

            var remaining = TimeSpan.FromTicks(_batchDeadline.Ticks - _timeProvider.GetUtcNow().Ticks);
            if (remaining <= TimeSpan.Zero)
                return 0;

            return Convert.ToInt32(Math.Min(remaining.TotalMilliseconds, int.MaxValue));
        }
    }

    private static void CompleteBatchWithFailure(List<DurabilityAck> batch, Exception ex)
    {
        // Flush failures fail the whole batch so no ack observes partial durability.
        for (var i = 0; i < batch.Count; i++)
        {
            var ack = batch[i];
            if (!ack.IsAbandonedByCaller())
                ack.SetException(ex);
        }

        for (var i = 0; i < batch.Count; i++)
        {
            if (batch[i].IsAbandonedByCaller())
                batch[i].ReturnToPool();
        }

        batch.Clear();
    }

    private static void CompleteBatchWithSuccess(List<DurabilityAck> batch)
    {
        for (var i = 0; i < batch.Count; i++)
        {
            var ack = batch[i];

            // Callers that canceled before the flush still own returning their ack to the pool.
            if (!ack.IsAbandonedByCaller())
                ack.SetResult();
        }

        for (var i = 0; i < batch.Count; i++)
        {
            if (batch[i].IsAbandonedByCaller())
                batch[i].ReturnToPool();
        }

        batch.Clear();
    }

    private bool CancelAck(DurabilityAck ack, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_sync)
        {
            removed = _acks.Remove(ack);
            if (removed && _acks.Count == 0)
                _batchDeadline.Clear();
        }

        if (removed)
            ack.SetCanceled(cancellationToken);

        return removed;
    }

    private void CompleteBatchOnJournalThread(List<DurabilityAck> batch)
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

    private bool TryTakeDueBatch(out List<DurabilityAck> batch)
    {
        lock (_sync)
        {
            if (_acks.Count == 0)
            {
                batch = _acks;
                return false;
            }

            var now = _timeProvider.GetUtcNow().Ticks;
            var due = _acks.Count >= _opt.JournalGroupCommitMaxBatch || now >= _batchDeadline.Ticks;
            if (!due)
            {
                batch = _acks;
                return false;
            }

            batch = _acks;
            _acks = _acksSpare;
            _acksSpare = batch;
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
