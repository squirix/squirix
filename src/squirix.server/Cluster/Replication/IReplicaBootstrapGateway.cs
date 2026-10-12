using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owner-side internode RPCs of the first start of a replica group toward its other members.</summary>
/// <remarks>The sender of the header is the owner of the group. Transport failures propagate to the caller, unretried and bounded only by the caller token.</remarks>
internal interface IReplicaBootstrapGateway
{
    /// <summary>Reads the replica status of one member of the group.</summary>
    /// <param name="nodeId">Target member node identifier.</param>
    /// <param name="header">Replication envelope identity of the owner.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The status the member reports.</returns>
    Task<ReplicaMemberStatus> GetStatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken);

    /// <summary>Offers one member of the group a place in the first start of the group.</summary>
    /// <param name="nodeId">Target member node identifier.</param>
    /// <param name="header">Replication envelope identity of the owner.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>Whether the member accepted the offer, with the state of its copy.</returns>
    Task<ReplicaBootstrapOfferResult> OfferAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken);
}
