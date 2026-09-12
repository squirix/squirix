using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Tracks admitted journal appends so a dead or wedged journal thread cannot leave them hanging
/// (issue #569). The journal thread and a failure drain race for each entry through a single lock:
/// whoever removes the entry owns the buffer release, the counter-decrement, and the ack.
/// Drained buffers are quarantined inside the registry until the journal thread is joined, closing
/// the check-then-write race where the thread could write into an already returned buffer.
/// </summary>
internal sealed class PendingAppendRegistry
{
    private readonly HashSet<TaskCompletionSource> _abortAcks = [];
    private readonly Dictionary<JournalWorkItem, PendingAppendEntry> _appends = new(ReferenceEqualityComparer.Instance);
    private readonly HashSet<TaskCompletionSource> _maintenanceAcks = [];
    private readonly Lock _sync = new();
    private Exception? _failure;
    private List<byte[]> _quarantine = [];
    private long _quarantinedCount;

    /// <summary>Gets the latched pipeline failure once any drain ran; otherwise <see langword="null" />.</summary>
    internal Exception? Failure
    {
        get
        {
            lock (_sync)
                return _failure;
        }
    }

    /// <summary>Gets the total number of buffers ever quarantined by drains (cumulative).</summary>
    internal long QuarantinedCount => Interlocked.Read(ref _quarantinedCount);

    /// <summary>
    /// Returns whether a drain took any staged item (see <see cref="IsAbandoned" />).
    /// A single lock covers the whole batch, so the gate observes one atomic drain state.
    /// </summary>
    /// <param name="items">The staged batch items to inspect.</param>
    /// <returns><see langword="true" /> when the whole batch must be dropped.</returns>
    internal bool AnyAbandoned(IReadOnlyList<JournalWorkItem> items)
    {
        lock (_sync)
        {
            for (var i = 0; i < items.Count; i++)
            {
                if (!_appends.ContainsKey(items[i]))
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Faults every tracked waiter (appends, maintenance, aborts) and closes the registry for
    /// appends and maintenance. Abort tracking stays open: the abort is diagnostics-only.
    /// Drained appends also release their queued-append slots here: the winning drain owns the
    /// counter-decrement exactly like the journal thread owns it on completion.
    /// Waiters drained here observe the original failure; producers arriving after the drain observe
    /// <see cref="InvalidOperationException" /> with the latched failure as its inner exception.
    /// </summary>
    /// <param name="failure">Failure reason propagated to pending acks and late arrivals.</param>
    /// <param name="logger">Logger for the drain summary.</param>
    /// <param name="queuedAppendsCounter">Queued-append slots released per drained append.</param>
    /// <returns>The number of drained appends.</returns>
    internal int FailAll(Exception failure, ILogger logger, MutableInt32 queuedAppendsCounter)
    {
        ArgumentNullException.ThrowIfNull(failure);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(queuedAppendsCounter);
        var entries = TakeAll(failure);
        var maintenance = TakeAllMaintenance(failure);

        // Fault everyone with the latched (first) failure, not this call's argument: a concurrent
        // earlier drain may have latched for a different reason, and one episode must report one error.
        var effective = Failure ?? failure;
        var bytes = 0L;
        for (var i = 0; i < entries.Count; i++)
        {
            _ = entries[i].Ack?.TrySetException(effective);
            _ = Interlocked.Decrement(ref queuedAppendsCounter.Value);
            bytes += entries[i].FrameLength;
        }

        for (var i = 0; i < maintenance.Count; i++)
            _ = maintenance[i].TrySetException(effective);

        var aborts = TakeAllAborts();
        for (var i = 0; i < aborts.Count; i++)
            _ = aborts[i].TrySetException(effective);

        if (entries.Count != 0)
            LogManager.JournalAbandonedAppendsDrained(logger, entries.Count, bytes);

        return entries.Count;
    }

    /// <summary>
    /// Returns whether the item was taken by a drain: every admitted append is tracked before it can
    /// reach the ring, so a missing entry means a drain already faulted it and took its buffer.
    /// Abandoned items must not be written or released.
    /// </summary>
    /// <param name="item">The journal work item to inspect.</param>
    /// <returns><see langword="true" /> when the item must not be written or released.</returns>
    internal bool IsAbandoned(JournalWorkItem item)
    {
        lock (_sync)
            return !_appends.ContainsKey(item);
    }

    /// <summary>Removes the maintenance abort ack without faulting it (bounded wait elapsed).</summary>
    /// <param name="ack">The abort ack to detach.</param>
    /// <returns><see langword="true" /> when the ack was still tracked.</returns>
    internal bool RemoveAbort(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        lock (_sync)
            return _abortAcks.Remove(ack);
    }

    /// <summary>
    /// Removes a maintenance ack for caller cancellation. Whoever wins the removal owns the outcome:
    /// the caller reports cancellation, otherwise the drain already faulted the waiter.
    /// </summary>
    /// <param name="ack">The maintenance ack to remove.</param>
    /// <returns><see langword="true" /> when the caller won and must cancel the waiter.</returns>
    internal bool RemoveMaintenance(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        lock (_sync)
            return _maintenanceAcks.Remove(ack);
    }

    /// <summary>
    /// Returns quarantined buffers to the pool. May only run after the journal thread was joined:
    /// with a live thread the buffers must stay quarantined.
    /// </summary>
    /// <returns>The number of buffers returned.</returns>
    internal int ReturnQuarantinedBuffers()
    {
        List<byte[]> taken;
        lock (_sync)
        {
            if (_quarantine.Count == 0)
                return 0;

            taken = _quarantine;
            _quarantine = [];
        }

        for (var i = 0; i < taken.Count; i++)
            ArrayPool<byte>.Shared.ReturnCleared(taken[i]);

        return taken.Count;
    }

    /// <summary>Drains tracked maintenance abort acks without closing the registry for them.</summary>
    /// <returns>Abort acks pending at drain time.</returns>
    internal List<TaskCompletionSource> TakeAllAborts()
    {
        lock (_sync)
        {
            if (_abortAcks.Count == 0)
                return [];

            var taken = new List<TaskCompletionSource>(_abortAcks);
            _abortAcks.Clear();
            return taken;
        }
    }

    /// <summary>
    /// Tracks an admitted append before it enters the ring. Must precede the ring enqueue (and its
    /// semaphore wait) so the fail-fast latch below, not the semaphore, bounds producers after a drain.
    /// Rethrows the latched failure itself; producer sites translate it via
    /// <c language="csharp">ThrowIfJournalThreadFailed</c> into <see cref="InvalidOperationException" />.
    /// </summary>
    /// <param name="item">Appending work item about to enter the ring.</param>
    /// <param name="frameBytes">Encoded frame buffer rented from the pool.</param>
    /// <param name="frameLength">Exact length of the framed payload inside <paramref name="frameBytes" />.</param>
    /// <param name="ack">Optional ack resolved on completion or failure.</param>
    internal void Track(JournalWorkItem item, byte[] frameBytes, int frameLength, TaskCompletionSource? ack)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(frameBytes);
        var entry = new PendingAppendEntry(item, frameBytes, frameLength, ack);
        lock (_sync)
        {
            // Admitting after a failure drain would park the entry on work nobody will
            // ever process: fail fast with the recorded reason instead of hanging.
            if (_failure != null)
                ExceptionDispatchInfo.Capture(_failure).Throw();

            _appends.Add(item, entry);
        }
    }

    /// <summary>Tracks a maintenance abort ack. Never fails fast: the abort is diagnostics-only.</summary>
    /// <param name="ack">The abort ack to track.</param>
    internal void TrackAbort(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        lock (_sync)
            _ = _abortAcks.Add(ack);
    }

    /// <summary>Tracks a maintenance ack before its item enters the ring.</summary>
    /// <param name="ack">The maintenance ack to track.</param>
    internal void TrackMaintenance(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        lock (_sync)
        {
            // Same fail-fast contract as appends: after a drain nobody processes the item.
            if (_failure != null)
                ExceptionDispatchInfo.Capture(_failure).Throw();

            _ = _maintenanceAcks.Add(ack);
        }
    }

    /// <summary>
    /// Removes an append for journal-thread completion. Whoever wins the removal owns the buffer
    /// release, the counter-decrement, and the ack; the loser (a drain) already did all three.
    /// </summary>
    /// <param name="item">The completed append work item.</param>
    /// <param name="entry">The owned entry, when the thread won.</param>
    /// <returns><see langword="true" /> when the thread won and must release and complete.</returns>
    internal bool Untrack(JournalWorkItem item, out PendingAppendEntry? entry)
    {
        lock (_sync)
        {
            if (!_appends.Remove(item, out entry))
            {
                entry = null;
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Drains all tracked appends for failure propagation and closes the registry.
    /// Taken buffers move to quarantine (never back to the pool: the thread may be alive).
    /// </summary>
    /// <param name="failure">Failure reason propagated to pending acks and late arrivals.</param>
    /// <returns>Appends pending at drain time.</returns>
    private List<PendingAppendEntry> TakeAll(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        lock (_sync)
        {
            _failure ??= failure;
            if (_appends.Count == 0)
                return [];

            var taken = new List<PendingAppendEntry>(_appends.Count);
            foreach (var entry in _appends.Values)
            {
                taken.Add(entry);
                _quarantine.Add(entry.FrameBytes);
            }

            _appends.Clear();
            _ = Interlocked.Add(ref _quarantinedCount, taken.Count);
            return taken;
        }
    }

    /// <summary>Drains tracked maintenance acks for failure propagation and closes the registry.</summary>
    /// <param name="failure">Failure reason propagated to pending acks and late arrivals.</param>
    /// <returns>Maintenance acks pending at drain time.</returns>
    private List<TaskCompletionSource> TakeAllMaintenance(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        lock (_sync)
        {
            _failure ??= failure;
            if (_maintenanceAcks.Count == 0)
                return [];

            var taken = new List<TaskCompletionSource>(_maintenanceAcks);
            _maintenanceAcks.Clear();
            return taken;
        }
    }
}
