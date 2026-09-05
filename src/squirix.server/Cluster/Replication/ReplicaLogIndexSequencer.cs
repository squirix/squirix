using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Serializes log-index assignment until the corresponding local appending is known to have succeeded or failed.</summary>
[ThreadSafe]
internal sealed class ReplicaLogIndexSequencer : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private ulong _nextIndex;

    internal ReplicaLogIndexSequencer(ulong lastLogIndex)
    {
        if (lastLogIndex == ulong.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(lastLogIndex), "Replica log index is exhausted.");

        _nextIndex = lastLogIndex + 1;
    }

    public void Dispose() => _gate.Dispose();

    internal async ValueTask<ReplicaIndexReservation> ReserveAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new ReplicaIndexReservation(this, _nextIndex);
    }

    internal void Complete(ulong index, bool appended)
    {
        try
        {
            if (index != _nextIndex)
                throw new InvalidOperationException("Replica log-index reservation no longer matches the next index.");
            if (!appended)
                return;
            if (_nextIndex == ulong.MaxValue)
                throw new InvalidOperationException("Replica log index is exhausted.");

            _nextIndex++;
        }
        finally
        {
            _ = _gate.Release();
        }
    }
}
