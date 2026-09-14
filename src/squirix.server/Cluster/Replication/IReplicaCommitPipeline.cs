using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Durable and memory operations required by the replication commit state machine.</summary>
internal interface IReplicaCommitPipeline
{
    /// <summary>Durably appends the leader copy.</summary>
    /// <param name="mutation">Prepared mutation.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken);

    /// <summary>Requests a durable append from one follower.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="mutation">Prepared mutation.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>The follower's durable acknowledgement.</returns>
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

    /// <summary>Records a replica that did not finish before majority commit.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="logIndex">Log index still requiring replication.</param>
    void RecordLaggingReplica(int replicaIndex, ulong logIndex);
}
