using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Exclusive reservation for the next dense range of group log indexes, starting at <see cref="Index" />.</summary>
[ThreadSafe]
internal sealed class ReplicaIndexReservation : IDisposable
{
    private ReplicaLogIndexSequencer? _owner;
    private AsyncLockHolder _holder;

    internal ReplicaIndexReservation(ReplicaLogIndexSequencer owner, AsyncLockHolder holder, ulong index, int count = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        _owner = owner;
        _holder = holder;
        Index = index;
        Count = count;
    }

    internal ulong Index { get; }

    /// <summary>Gets the number of dense indexes reserved, starting at <see cref="Index" />.</summary>
    internal int Count { get; }

    public void Dispose() => Finish(false);

    internal void MarkAppended() => Finish(true);

    private void Finish(bool appended)
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        if (owner == null)
            return;

        try
        {
            // Only an append advances the sequencer. A release without one leaves the index to the next reservation and
            // must not throw: it runs while a failed commit unwinds, and an exception here would replace the real cause.
            if (appended)
                owner.Complete(Index, Count);
        }
        finally
        {
            _holder.Dispose();
        }
    }
}
