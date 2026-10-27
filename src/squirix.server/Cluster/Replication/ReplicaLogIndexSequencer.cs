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

    internal ValueTask<ReplicaIndexReservation> ReserveAsync(CancellationToken cancellationToken) => ReserveAsync(1, cancellationToken);

    /// <summary>Reserves the next <paramref name="count" /> dense indexes, [next, next + count - 1], under one hold.</summary>
    /// <param name="count">Number of indexes; at least one.</param>
    /// <param name="cancellationToken">Cancellation token for the wait on the reservation gate.</param>
    /// <returns>The reservation, which advances the sequencer by <paramref name="count" /> only once marked appended.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count" /> is below one.</exception>
    /// <exception cref="InvalidOperationException">The range does not fit below the last log index.</exception>
    internal async ValueTask<ReplicaIndexReservation> ReserveAsync(int count, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        var holder = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        if (ulong.CreateChecked(count) <= ulong.MaxValue - _nextIndex)
            return new ReplicaIndexReservation(this, holder, _nextIndex, count);
        holder.Dispose();
        throw new InvalidOperationException("Replica log index is exhausted.");
    }

    internal void Complete(ulong index, int count = 1)
    {
        if (index != _nextIndex)
            throw new InvalidOperationException("Replica log-index reservation no longer matches the next index.");
        if (ulong.CreateChecked(count) > ulong.MaxValue - _nextIndex)
            throw new InvalidOperationException("Replica log index is exhausted.");

        _nextIndex += ulong.CreateChecked(count);
    }
}
