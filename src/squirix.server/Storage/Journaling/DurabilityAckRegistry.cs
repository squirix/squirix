using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability checkpoints for <see cref="JournalCoordinator" />.</summary>
internal sealed class DurabilityAckRegistry
{
    private readonly Lock _sync = new();
    private List<TaskCompletionSource> _acks = [];
    private Exception? _failure;

    internal int Count
    {
        get
        {
            lock (_sync)
                return _acks.Count;
        }
    }

    internal void Add(TaskCompletionSource ack)
    {
        lock (_sync)
        {
            // Admitting after a failure drain would park the waiter on a checkpoint nobody will
            // ever process: fail fast with the recorded reason instead of hanging.
            if (_failure != null)
                ExceptionDispatchInfo.Capture(_failure).Throw();

            _acks.Add(ack);
        }
    }

    internal bool Remove(TaskCompletionSource ack)
    {
        lock (_sync)
            return _acks.Remove(ack);
    }

    /// <summary>Drains all tracked checkpoints for failure propagation and closes the registry.</summary>
    /// <param name="failure">Failure reason propagated to pending acks and late arrivals.</param>
    /// <returns>Checkpoints pending at drain time.</returns>
    internal List<TaskCompletionSource> TakeAll(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        lock (_sync)
        {
            _failure ??= failure;
            if (_acks.Count == 0)
                return [];

            var taken = _acks;
            _acks = [];
            return taken;
        }
    }
}
