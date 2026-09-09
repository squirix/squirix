using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Exclusive reservation for the next group log index.</summary>
[ThreadSafe]
internal sealed class ReplicaIndexReservation : IDisposable
{
    private ReplicaLogIndexSequencer? _owner;
    private AsyncLockHolder _holder;

    internal ReplicaIndexReservation(ReplicaLogIndexSequencer owner, AsyncLockHolder holder, ulong index)
    {
        _owner = owner;
        _holder = holder;
        Index = index;
    }

    internal ulong Index { get; }

    public void Dispose() => Finish(false);

    internal void MarkAppended() => Finish(true);

    private void Finish(bool appended)
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        if (owner == null)
            return;

        try
        {
            owner.Complete(Index, appended);
        }
        finally
        {
            _holder.Dispose();
        }
    }
}
