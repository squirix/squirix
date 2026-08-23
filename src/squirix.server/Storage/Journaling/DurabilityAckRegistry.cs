using System;
using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability acks for <see cref="JournalCoordinator" />.</summary>
internal sealed class DurabilityAckRegistry
{
    private static readonly List<DurabilityAck> EmptyAcks = [];

    private readonly Lock _sync = new();
    private List<DurabilityAck> _acks = [];
    private List<DurabilityAck> _acksSpare = [];
    private Exception? _terminalFailure;

    /// <summary>
    /// Registers the ack unless the journal has already terminally failed. Returns <see langword="false" />
    /// in that case and completes the ack with the recorded failure, so a late registration can never
    /// strand a waiter whose enqueue would otherwise be accepted after the failure was drained.
    /// </summary>
    /// <param name="ack">The ack to register.</param>
    /// <returns><see langword="true" /> when registered; <see langword="false" /> when the journal already failed.</returns>
    internal bool TryRegister(DurabilityAck ack)
    {
        lock (_sync)
        {
            if (_terminalFailure is { } failure)
            {
                _ = ack.TrySetException(failure);
                return false;
            }

            _acks.Add(ack);
            return true;
        }
    }

    internal bool Remove(DurabilityAck ack)
    {
        lock (_sync)
            return _acks.Remove(ack);
    }

    /// <summary>
    /// Records the terminal failure and drains all registered acks under the same lock
    /// <see cref="TryRegister" /> uses: a concurrent registration either joins the drained set
    /// or is completed immediately with the failure.
    /// </summary>
    /// <param name="reason">The terminal failure to record and fault drained acks with.</param>
    /// <returns>The previously registered acks, drained for failure processing.</returns>
    internal List<DurabilityAck> Fail(Exception reason)
    {
        lock (_sync)
        {
            _terminalFailure = reason;
            return _acks.Count == 0 ? EmptyAcks : SwapOutAcks();
        }
    }

    internal List<DurabilityAck> TakeAll()
    {
        lock (_sync)
        {
            if (_acks.Count == 0)
                return EmptyAcks;

            return SwapOutAcks();
        }
    }

    private List<DurabilityAck> SwapOutAcks()
    {
        var batch = _acks;
        _acks = _acksSpare;
        _acks.Clear();
        _acksSpare = batch;
        return batch;
    }
}
