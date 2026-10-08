using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>One pre-vote or vote round of a candidate: every other member asked at once, each reply bounded by the vote timeout.</summary>
/// <remarks>
/// The round ends only when every member answered or timed out, so no reply arrives after it is counted. An unreachable or slow voter
/// is no vote, never a grant and never an observed term. The candidate counts for itself.
/// </remarks>
internal sealed class ReplicaVoteRound
{
    private readonly string[] _members;
    private readonly int _selfSlot;
    private readonly ReplicaGroupState _state;
    private readonly IReplicaVoteGateway _votes;

    /// <summary>Initializes a new instance of the <see cref="ReplicaVoteRound" /> class.</summary>
    /// <param name="votes">The vote RPCs.</param>
    /// <param name="members">The members of the group in slot order.</param>
    /// <param name="selfSlot">The slot of this node, which is not asked.</param>
    /// <param name="state">The election state, whose options and clock bound every reply.</param>
    internal ReplicaVoteRound(IReplicaVoteGateway votes, string[] members, int selfSlot, ReplicaGroupState state)
    {
        _votes = votes;
        _members = members;
        _selfSlot = selfSlot;
        _state = state;
    }

    /// <summary>Runs the round and counts it.</summary>
    /// <param name="preVote">Whether this is the pre-vote round, whose grants count at any reply term.</param>
    /// <param name="header">The envelope; its term is the proposed or candidate term.</param>
    /// <param name="last">The last entry of the candidate log.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the round by throwing.</param>
    /// <returns>
    /// The grants, this node included, and the highest term a refusal reported. A vote counts only when it is granted with a reply term
    /// equal to the candidate term; a pre-vote counts when it is granted.
    /// </returns>
    internal async Task<(int Granted, ulong Highest)> RunAsync(bool preVote, ReplicaRpcHeader header, (ulong Index, ulong Term) last, CancellationToken cancellationToken)
    {
        var calls = new Task<FollowerLogVoteResult?>[_members.Length];
        for (var i = 0; i < calls.Length; i++)
            calls[i] = i == _selfSlot ? Task.FromResult<FollowerLogVoteResult?>(null) : AskAsync(preVote, _members[i], header, last, cancellationToken);

        var replies = await Task.WhenAll(calls).ConfigureAwait(false);
        var granted = 1;
        var highest = 0UL;
        for (var i = 0; i < replies.Length; i++)
        {
            if (replies[i] is not { } reply)
                continue;

            if (reply.Granted && (preVote || reply.CurrentTerm == header.Term))
                granted++;
            else if (reply.CurrentTerm > highest)
                highest = reply.CurrentTerm;
        }

        return (granted, highest);
    }

    private async Task<FollowerLogVoteResult?> AskAsync(bool preVote, string nodeId, ReplicaRpcHeader header, (ulong Index, ulong Term) last, CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(_state.Options.VoteRpcTimeout, _state.Clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return preVote ? await _votes.PreVoteAsync(nodeId, header, last.Index, last.Term, linked.Token).ConfigureAwait(false)
                : await _votes.RequestVoteAsync(nodeId, header, last.Index, last.Term, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception exception) when (exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException)
        {
            return null;
        }
    }
}
