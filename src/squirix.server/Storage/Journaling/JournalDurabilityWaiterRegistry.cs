using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability waiters for <see cref="JournalCoordinator" />.</summary>
internal sealed class JournalDurabilityWaiterRegistry
{
    private static readonly List<JournalDurabilityWaiter> EmptyWaiters = [];

    private readonly Lock _sync = new();
    private List<JournalDurabilityWaiter> _waiters = [];
    private List<JournalDurabilityWaiter> _waitersSpare = [];

    internal void Add(JournalDurabilityWaiter waiter)
    {
        lock (_sync)
            _waiters.Add(waiter);
    }

    internal bool Remove(JournalDurabilityWaiter waiter)
    {
        lock (_sync)
            return _waiters.Remove(waiter);
    }

    internal List<JournalDurabilityWaiter> TakeAll()
    {
        lock (_sync)
        {
            if (_waiters.Count == 0)
                return EmptyWaiters;

            return SwapOutWaiters();
        }
    }

    internal bool TryTakeAllIfAny(out List<JournalDurabilityWaiter> waiters)
    {
        lock (_sync)
        {
            if (_waiters.Count == 0)
            {
                waiters = EmptyWaiters;
                return false;
            }

            waiters = SwapOutWaiters();
            return true;
        }
    }

    private List<JournalDurabilityWaiter> SwapOutWaiters()
    {
        var batch = _waiters;
        _waiters = _waitersSpare;
        _waiters.Clear();
        _waitersSpare = batch;
        return batch;
    }
}
