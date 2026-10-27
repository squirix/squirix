using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Leader-side internode RPC that sends a state transfer to a member whose copy of the group is behind.</summary>
/// <remarks>
/// <para>
/// The sender of the header is the leader of the group, and its term is the term of the transfer. A transfer is one begin, then the messages in order with
/// the sequence number rising by one, then an end; an abort ends it early. The gateway does not chunk, retry or reorder: the caller keeps each message within
/// <see cref="ReplicaTransferOptions.MessageBudgetBytes" />. A refusal of the receiver is a reply, not an exception; transport failures propagate unretried
/// and bounded only by the caller token. The buffers of a message must stay unchanged until the returned task completes.
/// </para>
/// </remarks>
internal interface IReplicaTransferGateway
{
    /// <summary>Begins a transfer on one member of the group.</summary>
    /// <param name="nodeId">Target member node identifier.</param>
    /// <param name="header">Replication envelope identity of the leader; the fingerprint and the generation it carries are those of the baseline.</param>
    /// <param name="transferId">The identifier every message of the transfer carries.</param>
    /// <param name="baseline">The point the transfer is taken at and the snapshot image that follows; its fingerprint and generation are taken from the header.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The reply of the receiver.</returns>
    Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken);

    /// <summary>Sends one message after the begin.</summary>
    /// <param name="nodeId">Target member node identifier.</param>
    /// <param name="header">Replication envelope identity of the leader; it names the group.</param>
    /// <param name="message">The message; its sequence number and its offset must not be negative.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The reply of the receiver.</returns>
    Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken);

    /// <summary>Aborts a transfer; the copy of the member stays as it is.</summary>
    /// <param name="nodeId">Target member node identifier.</param>
    /// <param name="header">Replication envelope identity of the leader; it names the group.</param>
    /// <param name="transferId">The transfer to abort.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The reply of the receiver.</returns>
    Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken);
}
