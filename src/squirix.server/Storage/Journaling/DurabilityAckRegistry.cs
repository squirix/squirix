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

    internal void Add(DurabilityAck ack)
    {
        lock (_sync)
            _acks.Add(ack);
    }

    internal bool Remove(DurabilityAck ack)
    {
        lock (_sync)
            return _acks.Remove(ack);
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
