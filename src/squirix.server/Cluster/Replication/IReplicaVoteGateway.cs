using System;
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
    /// <summary>Dispatches one election ballot with its reply budget and the round's cancellation token.</summary>
    /// <param name="preVote">Whether to ask for a pre-vote.</param>
    /// <param name="nodeId">The target voter.</param>
    /// <param name="header">The replication envelope.</param>
    /// <param name="last">The index and term of the candidate's last durable log entry.</param>
    /// <param name="replyBudget">The duration and clock for the ballot exchange.</param>
    /// <param name="cancellationToken">The token for the whole call, including connection establishment.</param>
    /// <returns>The voter's reply.</returns>
    async Task<FollowerLogVoteResult> AskVoteAsync(
        bool preVote,
        string nodeId,
        ReplicaRpcHeader header,
        (ulong Index, ulong Term) last,
        (TimeSpan Timeout, TimeProvider Clock) replyBudget,
        CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(replyBudget.Timeout, replyBudget.Clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        return preVote
            ? await PreVoteAsync(nodeId, header, last.Index, last.Term, linked.Token).ConfigureAwait(false)
            : await RequestVoteAsync(nodeId, header, last.Index, last.Term, linked.Token).ConfigureAwait(false);
    }

    Task SendTransferAsync(string target, ReplicaRpcHeader header, CancellationToken cancellationToken) => Task.CompletedTask;

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
