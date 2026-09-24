using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Batches journal durability flushes so concurrent mutations can share one fsync while each ack
/// still observes durability before in-memory apply. Deadline evaluation runs on the journal thread.
/// </summary>
internal sealed class JournalDurabilityGroupCommit
{
    private readonly BatchDeadline _batchDeadline = new();
    private readonly Action _journalThreadFlush;
    private readonly Action<string>? _onWaitCanceled;
    private readonly Action _notifyJournalThread;
    private readonly PersistenceOptions _opt;
    private readonly Lock _sync = new();
    private readonly TimeProvider _timeProvider;

    private List<TaskCompletionSource> _acks;
    private List<TaskCompletionSource> _acksSpare;
    private Exception? _failure;
    private List<TaskCompletionSource>? _inFlight;

    internal JournalDurabilityGroupCommit(Action journalThreadFlush, Action notifyJournalThread, PersistenceOptions opt, TimeProvider? timeProvider = null, Action<string>? onWaitCanceled = null)
    {
        ArgumentNullException.ThrowIfNull(journalThreadFlush);
        ArgumentNullException.ThrowIfNull(notifyJournalThread);
        ArgumentNullException.ThrowIfNull(opt);
        _journalThreadFlush = journalThreadFlush;
        _notifyJournalThread = notifyJournalThread;
        _opt = opt;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _onWaitCanceled = onWaitCanceled;

        var capacity = Math.Max(4, opt.JournalGroupCommitMaxBatch);
        _acks = [with(capacity)];
        _acksSpare = [with(capacity)];
    }

    /// <summary>Waits until appended journal bytes through the caller's append are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for the caller's batch.</returns>
    internal async ValueTask AwaitCommitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Each waiter owns its completion source. Cancellation removes it while the batch is pending.
        // After the journal thread takes the batch, cancellation only affects the WaitAsync caller.
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var signalJournal = false;
            lock (_sync)
            {
                // Admitting after CancelPending would park the waiter on a batch nobody will ever
                // drain (journal thread exiting or pipeline failed): fail fast with the recorded
                // reason instead of hanging.
                if (_failure != null)
                    ExceptionDispatchInfo.Capture(_failure).Throw();

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

            await ack.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _onWaitCanceled?.Invoke("group commit");
            CancelAck(ack, cancellationToken);
            throw;
        }
    }

    /// <summary>Fails any pending or in-flight commit acks during shutdown or journal pipeline failure.</summary>
    /// <param name="reason">Failure reason propagated to the acks.</param>
    /// <returns>Number of in-flight acks this call faulted.</returns>
    internal int CancelPending(Exception reason) => CancelPendingCore(reason);

    internal int CancelPendingCore(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (_sync)
        {
            _failure ??= reason;
            _batchDeadline.Clear();
            _acks.FaultAll(reason);
            _acks.Clear();

            // The batch taken by the journal thread stays reachable until the thread writes its outcome:
            // fault it without clearing, because the thread owns that list and swaps it back as the spare.
            return _inFlight?.FaultPending(reason) ?? 0;
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
            if (_acks.Count == 0 || !_batchDeadline.IsArmed)
                return Timeout.Infinite;

            var remaining = TimeSpan.FromTicks(_batchDeadline.Ticks - _timeProvider.GetUtcNow().Ticks);
            return remaining <= TimeSpan.Zero ? 0 : Convert.ToInt32(Math.Min(remaining.TotalMilliseconds, int.MaxValue));
        }
    }

    private void CancelAck(TaskCompletionSource ack, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_sync)
        {
            removed = _acks.Remove(ack);
            if (removed && _acks.Count == 0)
                _batchDeadline.Clear();
        }

        // When the batch was already taken by the journal thread there is nothing to cancel:
        // the thread resolves the source and the canceled waiter already observed via WaitAsync.
        if (removed)
            _ = ack.TrySetCanceled(cancellationToken);
    }

    private void CompleteBatchOnJournalThread(List<TaskCompletionSource> batch)
    {
        try
        {
            // One journal-thread fsync covers every ack captured in this due batch. It runs outside the
            // lock, so a drain can still fault the in-flight batch while the fsync never returns.
            _journalThreadFlush();
        }
        catch (Exception ex)
        {
            // Flush failures fail the whole batch so no ack observes partial durability. The rethrow
            // fails the journal pipeline: a later fsync can succeed after the kernel dropped the dirty
            // pages of the failed one, so the flush must never be retried and reported as durable.
            lock (_sync)
            {
                batch.FaultAll(ex);
                ReleaseInFlight(batch);
            }

            throw;
        }

        // Completing under the lock is safe: the sources run their continuations asynchronously.
        lock (_sync)
        {
            batch.CompleteAll();
            ReleaseInFlight(batch);
        }
    }

    private void ReleaseInFlight(List<TaskCompletionSource> batch)
    {
        batch.Clear();
        _inFlight = null;
    }

    private bool TryTakeDueBatch(out List<TaskCompletionSource> batch)
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
            _inFlight = batch;
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
