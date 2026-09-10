using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks in-flight durability checkpoints for <see cref="JournalCoordinator" />.</summary>
internal sealed class DurabilityAckRegistry
{
    private readonly Lock _sync = new();
    private List<TaskCompletionSource> _acks = [];

    internal void Add(TaskCompletionSource ack)
    {
        lock (_sync)
            _acks.Add(ack);
    }

    internal bool Remove(TaskCompletionSource ack)
    {
        lock (_sync)
            return _acks.Remove(ack);
    }

    internal List<TaskCompletionSource> TakeAll()
    {
        lock (_sync)
        {
            if (_acks.Count == 0)
                return [];

            var taken = _acks;
            _acks = [];
            return taken;
        }
    }
}
