using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Candidate-side internode election RPCs toward the voters of one replica group.</summary>
/// <remarks>
/// The candidate is the sender of the header; the voter binds it to the peer certificate and ignores the leader identity.
/// Transport failures propagate to the caller, unretried and bounded only by the caller token: an exception is no vote, never a
/// grant and never an observed term. How a reply counts differs per call; see each method.
/// </remarks>
internal interface IReplicaVoteGateway
{
    /// <summary>Asks one voter whether it would grant a vote for the term the candidate proposes, without changing the voter.</summary>
    /// <param name="nodeId">Target voter node identifier.</param>
    /// <param name="header">Replication envelope identity; its term is the proposed term.</param>
    /// <param name="lastLogIndex">The candidate durable last log index.</param>
    /// <param name="lastLogTerm">The term at the candidate durable last log index.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The voter answer with its unchanged durable term; term zero when it refused before reaching its log.</returns>
    /// <remarks>
    /// A probe counts when it is granted: its reply term is the voter's unchanged term, usually below the proposed term. A reply
    /// term above the proposed term is a term observation, never a grant.
    /// </remarks>
    Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken);

    /// <summary>Asks one voter for its vote in the candidate term.</summary>
    /// <param name="nodeId">Target voter node identifier.</param>
    /// <param name="header">Replication envelope identity; its term is the candidate persisted term.</param>
    /// <param name="lastLogIndex">The candidate durable last log index.</param>
    /// <param name="lastLogTerm">The term at the candidate durable last log index.</param>
    /// <param name="cancellationToken">Cancellation token bounding the call.</param>
    /// <returns>The voter answer with its durable term after the request; term zero when it refused before reaching its log.</returns>
    /// <remarks>A vote counts only when it is granted and its reply term equals the term of <paramref name="header" />.</remarks>
    Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken);
}
