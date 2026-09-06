using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owner-side internode replication RPCs toward follower replicas.</summary>
/// <remarks>
/// Transport failures propagate to the caller: the commit coordinator observes faulted follower
/// tasks and treats them as lagging replicas, so a slow or dead follower never fails a commit
/// that still holds a durable majority.
/// </remarks>
internal interface IReplicaRpcGateway
{
    /// <summary>Appends entries to one follower's group log.</summary>
    /// <param name="nodeId">Target follower node identifier.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="batch">Leader batch with identity, consistency, and commit positions.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The follower append outcome.</returns>
    Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken);

    /// <summary>Advances one follower's group commit index.</summary>
    /// <param name="nodeId">Target follower node identifier.</param>
    /// <param name="header">Replication envelope identity.</param>
    /// <param name="commitIndex">Target commit index.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The commit advance outcome.</returns>
    Task<FollowerLogCommitResult> AdvanceCommitAsync(string nodeId, ReplicaRpcHeader header, ulong commitIndex, CancellationToken cancellationToken);
}
