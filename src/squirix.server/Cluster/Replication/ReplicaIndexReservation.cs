using System;
using System.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Exclusive reservation for the next group log index.</summary>
internal sealed class ReplicaIndexReservation : IDisposable
{
    private ReplicaLogIndexSequencer? _owner;

    internal ReplicaIndexReservation(ReplicaLogIndexSequencer owner, ulong index)
    {
        _owner = owner;
        Index = index;
    }

    internal ulong Index { get; }

    public void Dispose() => Finish(false);

    internal void MarkAppended() => Finish(true);

    private void Finish(bool appended)
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        owner?.Complete(Index, appended);
    }
}
