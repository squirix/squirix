using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Tracks durability checkpoints for <see cref="JournalCoordinator" /> until the journal thread completes them. A checkpoint is
/// either pending (its caller may still remove it and report cancellation) or in flight (its fsync is running and only the journal
/// thread may complete it). The failure and shutdown drains reach both states.
/// </summary>
internal sealed class DurabilityAckRegistry
{
    private readonly Lock _sync = new();
    private List<TaskCompletionSource> _pending = [];
    private List<TaskCompletionSource> _inFlight = [];
    private Exception? _failure;

    internal void Add(TaskCompletionSource ack)
    {
        lock (_sync)
        {
            // Admitting after a failure drain would park the waiter on a checkpoint nobody will
            // ever process: fail fast with the recorded reason instead of hanging.
            if (_failure != null)
                ExceptionDispatchInfo.Capture(_failure).Throw();

            _pending.Add(ack);
        }
    }

    /// <summary>Releases an in-flight checkpoint once the journal thread wrote its outcome.</summary>
    /// <param name="ack">Checkpoint previously marked by <see cref="TryMarkInFlight" />.</param>
    internal void Complete(TaskCompletionSource ack)
    {
        lock (_sync)
            _ = _inFlight.Remove(ack);
    }

    /// <summary>Removes a pending checkpoint; an in-flight one belongs to the journal thread and is never removed here.</summary>
    /// <param name="ack">Checkpoint to remove.</param>
    /// <returns>Whether the checkpoint was pending and is now owned by the caller.</returns>
    internal bool Remove(TaskCompletionSource ack)
    {
        lock (_sync)
            return _pending.Remove(ack);
    }

    /// <summary>Drains all tracked checkpoints for failure propagation and closes the registry.</summary>
    /// <param name="failure">Failure reason propagated to tracked acks and late arrivals.</param>
    /// <returns>Checkpoints pending or in flight at drain time.</returns>
    internal List<TaskCompletionSource> TakeAll(Exception failure) => TakeAll(failure, out _);

    /// <summary>Drains all tracked checkpoints for failure propagation and closes the registry.</summary>
    /// <param name="failure">Failure reason propagated to tracked acks and late arrivals.</param>
    /// <param name="inFlightCount">Number of drained checkpoints whose fsync was running; they end the returned list.</param>
    /// <returns>Checkpoints pending at drain time, followed by the checkpoints in flight.</returns>
    internal List<TaskCompletionSource> TakeAll(Exception failure, out int inFlightCount)
    {
        ArgumentNullException.ThrowIfNull(failure);
        lock (_sync)
        {
            _failure ??= failure;
            inFlightCount = _inFlight.Count;
            if (_pending.Count == 0 && inFlightCount == 0)
                return [];

            // The in-flight entries leave the registry with the drain; the journal thread still writes
            // their outcome, which is a no-op on a source the drain already faulted.
            var taken = _pending;
            taken.AddRange(_inFlight);
            _pending = [];
            _inFlight = [];
            return taken;
        }
    }

    /// <summary>Hands a pending checkpoint to the journal thread right before its fsync, so no caller can remove it anymore.</summary>
    /// <param name="ack">Checkpoint carried by the work item being processed.</param>
    /// <returns>Whether the checkpoint was pending; <see langword="false" /> when a caller detached it or a drain already faulted it.</returns>
    internal bool TryMarkInFlight(TaskCompletionSource ack)
    {
        lock (_sync)
        {
            if (!_pending.Remove(ack))
                return false;

            _inFlight.Add(ack);
            return true;
        }
    }
}
