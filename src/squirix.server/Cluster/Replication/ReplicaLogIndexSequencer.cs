using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Serializes log-index assignment until the corresponding local appending is known to have succeeded or failed.</summary>
[ThreadSafe]
internal sealed class ReplicaLogIndexSequencer : IDisposable
{
    private readonly AsyncLock _gate = new();
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
        var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (_nextIndex != ulong.MaxValue)
            return new ReplicaIndexReservation(this, holder, _nextIndex);
        holder.Dispose();
        throw new InvalidOperationException("Replica log index is exhausted.");
    }

    internal void Complete(ulong index, bool appended)
    {
        if (index != _nextIndex)
            throw new InvalidOperationException("Replica log-index reservation no longer matches the next index.");
        if (!appended)
            return;
        if (_nextIndex == ulong.MaxValue)
            throw new InvalidOperationException("Replica log index is exhausted.");

        _nextIndex++;
    }
}
