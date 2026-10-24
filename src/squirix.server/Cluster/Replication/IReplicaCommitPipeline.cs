using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Durable and memory operations required by the replication commit state machine.</summary>
internal interface IReplicaCommitPipeline
{
    /// <summary>Durably appends the leader copy of a dense, ordered run of entries in one log request.</summary>
    /// <param name="mutations">Prepared mutations with consecutive log indexes in one term; at least one.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken);

    /// <summary>Requests a durable append from one follower.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="mutation">Prepared mutation; any entry of the last locally appended run.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>The follower's durable acknowledgement of that entry.</returns>
    /// <remarks>
    /// An implementation may ignore <paramref name="cancellationToken" />: the entry is already in the local log, so it must always reach
    /// the follower in order, and a late acknowledgement must still be observed. The commit stops at its budget while it waits for the
    /// majority, not through this call.
    /// </remarks>
    ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken);

    /// <summary>Durably advances the local group commit index.</summary>
    /// <param name="commitIndex">New contiguous commit index.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken);

    /// <summary>Applies a committed mutation to memory.</summary>
    /// <param name="mutation">Prepared mutation.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken);

    /// <summary>Records a follower whose append of an entry failed, was refused, or could not be counted; it stops counting until it is caught up.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="logIndex">Log index of the entry the follower did not acknowledge.</param>
    void RecordLaggingReplica(int replicaIndex, ulong logIndex);
}
