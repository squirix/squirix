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

    private List<PooledAck> _acks;
    private List<PooledAck> _acksSpare;

    internal JournalDurabilityGroupCommit(Action journalThreadFlush, Action notifyJournalThread, PersistenceOptions opt, TimeProvider? timeProvider = null)
    {
        _journalThreadFlush = journalThreadFlush ?? throw new ArgumentNullException(nameof(journalThreadFlush));
        _notifyJournalThread = notifyJournalThread ?? throw new ArgumentNullException(nameof(notifyJournalThread));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _timeProvider = timeProvider ?? TimeProvider.System;

        var capacity = Math.Max(4, opt.JournalGroupCommitMaxBatch);
        _acks = new List<PooledAck>(capacity);
        _acksSpare = new List<PooledAck>(capacity);
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for the caller's batch.</returns>
    internal async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var ack = PooledAck.Rent();
        var ackWaitTask = ack.WaitAsync(cancellationToken);
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

        try
        {
            await ackWaitTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The ack is caller-owned only while it is still pending (not yet captured into a journal batch).
            // If the journal already took it, the journal owns and pools it; the caller must not.
            if (!TryRemovePending(ack))
                throw;
            _ = ack.TrySetCanceled(cancellationToken);
            ack.Return();

            throw;
        }
    }

    /// <summary>Fails any pending commit acks during shutdown or journal pipeline failure.</summary>
    /// <param name="reason">Failure reason propagated to pending acks.</param>
    /// <returns>A completed task once pending acks are failed.</returns>
    internal ValueTask CancelPendingAsync(Exception reason)
    {
        CancelPendingCore(reason);
        return ValueTask.CompletedTask;
    }

    internal void CancelPendingCore(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (_sync)
        {
            _batchDeadline.Clear();

            // These acks were never captured into a journal batch, so the caller side owns and pools them.
            for (var i = 0; i < _acks.Count; i++)
                _ = _acks[i].TrySetException(reason);

            for (var i = 0; i < _acks.Count; i++)
                _acks[i].Return();

            _acks.Clear();
        }

        // Acks already captured into a journal batch are pooled by the journal (CompleteBatchWithSuccess /
        // CompleteBatchWithFailure); they are no longer in _acks and are not touched here.
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

    private static void CompleteBatchWithFailure(List<PooledAck> batch, Exception ex)
    {
        // Flush failures fail the whole batch so no ack observes partial durability. The journal owns every
        // ack captured in the batch, so it sets the terminal result and is the single pool point.
        for (var i = 0; i < batch.Count; i++)
            _ = batch[i].TrySetException(ex);

        for (var i = 0; i < batch.Count; i++)
            batch[i].Return();

        batch.Clear();
    }

    private static void CompleteBatchWithSuccess(List<PooledAck> batch)
    {
        for (var i = 0; i < batch.Count; i++)
            _ = batch[i].TrySetResult();

        for (var i = 0; i < batch.Count; i++)
            batch[i].Return();

        batch.Clear();
    }

    private bool TryRemovePending(PooledAck ack)
    {
        lock (_sync)
        {
            if (!_acks.Remove(ack))
                return false;

            if (_acks.Count == 0)
                _batchDeadline.Clear();

            return true;
        }
    }

    private void CompleteBatchOnJournalThread(List<PooledAck> batch)
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

    private bool TryTakeDueBatch(out List<PooledAck> batch)
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
