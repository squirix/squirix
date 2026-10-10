using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>One pre-vote or vote round of a candidate: every other member asked at once, each reply bounded by the vote timeout.</summary>
/// <remarks>
/// The round ends once its outcome is decided: a majority granted, a majority can no longer be reached, or a reply reported a term above
/// the candidate's. Calls still unanswered are canceled and awaited, so no reply arrives after the round ended and a silent member adds
/// no vote timeout to a decided round. An unreachable or slow voter is no vote, never a grant and never an observed term. The candidate
/// counts for itself.
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
    /// <param name="ownTerm">The durable term of the candidate; a reply reporting a higher term ends the round.</param>
    /// <param name="last">The last entry of the candidate log.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the round by throwing.</param>
    /// <returns>
    /// The grants, this node included, and the highest term a refusal reported. A vote counts only when it is granted with a reply term
    /// equal to the candidate term; a pre-vote counts when it is granted.
    /// </returns>
    /// <remarks>No call outlives the round: the ones still unanswered once the outcome is decided are canceled and awaited.</remarks>
    internal async Task<(int Granted, ulong Highest)> RunAsync(
        bool preVote,
        ReplicaRpcHeader header,
        ulong ownTerm,
        (ulong Index, ulong Term) last,
        CancellationToken cancellationToken)
    {
        var majority = (_members.Length / 2) + 1;
        var calls = new Task<FollowerLogVoteResult?>[_members.Length];
        var tallied = new bool[_members.Length];
        var pending = new List<Task<FollowerLogVoteResult?>>(_members.Length);
        using var round = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        for (var i = 0; i < calls.Length; i++)
        {
            if (i == _selfSlot)
            {
                calls[i] = Task.FromResult<FollowerLogVoteResult?>(null);
                continue;
            }

            calls[i] = AskAsync(preVote, _members[i], header, last, cancellationToken, round.Token);
            pending.Add(calls[i]);
        }

        var granted = 1;
        var highest = 0UL;
        while (pending.Count > 0 && granted < majority && granted + pending.Count >= majority && highest <= ownTerm)
        {
            var finished = await Task.WhenAny(pending).ConfigureAwait(false);
            _ = pending.Remove(finished);
            if (!finished.IsCompletedSuccessfully)
                break;

            tallied[Array.IndexOf(calls, finished)] = true;
            Tally(await finished.ConfigureAwait(false), preVote, header.Term, ref granted, ref highest);
        }

        // Calls given up are canceled and awaited, so none outlives the round; a faulted one rethrows here.
        await round.CancelAsync().ConfigureAwait(false);
        var replies = await Task.WhenAll(calls).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        for (var i = 0; i < replies.Length; i++)
        {
            if (!tallied[i])
                Tally(replies[i], preVote, header.Term, ref granted, ref highest);
        }

        return (granted, highest);
    }

    private static void Tally(FollowerLogVoteResult? result, bool preVote, ulong term, ref int granted, ref ulong highest)
    {
        if (result is not { } reply)
            return;

        if (reply.Granted && (preVote || reply.CurrentTerm == term))
            granted++;
        else if (reply.CurrentTerm > highest)
            highest = reply.CurrentTerm;
    }

    private async Task<FollowerLogVoteResult?> AskAsync(
        bool preVote,
        string nodeId,
        ReplicaRpcHeader header,
        (ulong Index, ulong Term) last,
        CancellationToken cancellationToken,
        CancellationToken giveUp)
    {
        using var timeout = new CancellationTokenSource(_state.Options.VoteRpcTimeout, _state.Clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(giveUp, timeout.Token);
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
