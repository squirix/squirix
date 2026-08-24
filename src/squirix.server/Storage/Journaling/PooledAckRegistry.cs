using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability acks for <see cref="JournalCoordinator" />.</summary>
internal sealed class PooledAckRegistry
{
    private static readonly List<PooledAck> EmptyAcks = [];

    private readonly Lock _sync = new();
    private List<PooledAck> _acks = [];
    private List<PooledAck> _acksSpare = [];

    internal void Add(PooledAck ack)
    {
        lock (_sync)
            _acks.Add(ack);
    }

    internal bool Remove(PooledAck ack)
    {
        lock (_sync)
            return _acks.Remove(ack);
    }

    internal List<PooledAck> TakeAll()
    {
        lock (_sync)
        {
            if (_acks.Count == 0)
                return EmptyAcks;

            return SwapOutAcks();
        }
    }

    private List<PooledAck> SwapOutAcks()
    {
        var batch = _acks;
        _acks = _acksSpare;
        _acks.Clear();
        _acksSpare = batch;
        return batch;
    }
}
