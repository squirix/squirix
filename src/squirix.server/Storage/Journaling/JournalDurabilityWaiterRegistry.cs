using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability waiters for <see cref="JournalCoordinator" />.</summary>
internal sealed class JournalDurabilityWaiterRegistry
{
    private readonly List<JournalDurabilityWaiter> _waiters = [];
    private readonly Lock _sync = new();

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
            if (_waiters.Count is 0)
                return [];

            var waiters = new List<JournalDurabilityWaiter>(_waiters);
            _waiters.Clear();
            return waiters;
        }
    }

    internal bool TryTakeAllIfAny(out List<JournalDurabilityWaiter> waiters)
    {
        lock (_sync)
        {
            if (_waiters.Count is 0)
            {
                waiters = [];
                return false;
            }

            waiters = new List<JournalDurabilityWaiter>(_waiters);
            _waiters.Clear();
            return true;
        }
    }
}
