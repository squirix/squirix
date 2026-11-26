using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Counts the election rounds the nodes of a test cluster ran, so a test can wait for campaigns that must fail instead of a fixed time.</summary>
/// <remarks>
/// A campaign starts with a pre-vote round that asks every other member of the group at once and awaits every call. A pre-vote call that
/// finished, whatever its outcome, counts once for its sender, group and target, so the pre-vote rounds a node ran in a group are the largest
/// count over its targets. One counter is registered on every node of the cluster; the sender of each call is the node that made it.
/// </remarks>
[ThreadSafe]
internal sealed class ElectionRoundCounter
{
    private readonly ConcurrentDictionary<(string GroupId, string Sender, string Target), int> _preVotes = new();

    internal Action<string, string, string, string, ulong>? Observe { get; init; }

    internal Action<string, string, string, ulong, string>? ObservePreVoteResult { get; init; }

    internal Func<string, string, string, ulong, Activity?>? TracePreVote { get; init; }

    internal bool DropHints { get; init; }

    internal string[]? BroadcastMembers { get; init; }

    /// <summary>Gets the number of pre-vote rounds a node ran in a group so far.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="nodeId">The campaigning node.</param>
    /// <returns>The number of pre-vote rounds the node finished.</returns>
    internal int PreVoteRounds(string groupId, string nodeId)
    {
        var rounds = 0;
        foreach (var (key, count) in _preVotes)
        {
            if (count > rounds && string.Equals(key.GroupId, groupId, StringComparison.Ordinal) && string.Equals(key.Sender, nodeId, StringComparison.Ordinal))
                rounds = count;
        }

        return rounds;
    }

    /// <summary>Starts counting the pre-vote rounds of the given nodes from now on.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="nodes">The campaigning nodes.</param>
    /// <param name="rounds">The pre-vote rounds each node must finish after this call.</param>
    /// <returns>A condition that holds once every node finished the given number of pre-vote rounds in the group since this call.</returns>
    internal Func<bool> RoundsAfterNow(string groupId, string[] nodes, int rounds)
    {
        var baseline = new int[nodes.Length];
        for (var i = 0; i < nodes.Length; i++)
            baseline[i] = PreVoteRounds(groupId, nodes[i]);

        return () =>
        {
            for (var i = 0; i < nodes.Length; i++)
            {
                if (PreVoteRounds(groupId, nodes[i]) - baseline[i] < rounds)
                    return false;
            }

            return true;
        };
    }

    /// <summary>Replaces the vote gateway of a node with one that counts the pre-vote calls it makes and delegates to the product gateway.</summary>
    /// <param name="services">The node service collection.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> is <see langword="null" />.</exception>
    internal void Register(TestServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _ = services.RemoveAll<IReplicaVoteGateway>().AddSingleton<IReplicaVoteGateway>(sp => new CountingVoteGateway(sp.GetRequiredService<ReplicaRpcGateway>(), this));
    }

    private void CountPreVote(string groupId, string sender, string target) =>
        _ = _preVotes.AddOrUpdate((groupId, sender, target), 1, static (_, count) => count + 1);

    [Immutable]
    private sealed class CountingVoteGateway : IReplicaVoteGateway
    {
        private readonly ElectionRoundCounter _counter;
        private readonly IReplicaVoteGateway _inner;

        internal CountingVoteGateway(IReplicaVoteGateway inner, ElectionRoundCounter counter)
        {
            _inner = inner;
            _counter = counter;
        }

        public Task SendTransferAsync(string target, ReplicaRpcHeader header, CancellationToken cancellationToken)
        {
            if (_counter.BroadcastMembers is not { } members)
                return SendHintAsync(target, in header, cancellationToken);

            var calls = new System.Collections.Generic.List<Task>();
            foreach (var member in members)
            {
                if (!string.Equals(member, header.SenderNodeId, StringComparison.Ordinal))
                    calls.Add(SendHintAsync(member, in header, cancellationToken));
            }

            return Task.WhenAll(calls);
        }

        public async Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
        {
            using var trace = _counter.TracePreVote?.Invoke(header.GroupId, header.SenderNodeId, nodeId, header.Term);
            _counter.Observe?.Invoke(header.GroupId, header.SenderNodeId, nodeId, header.TransferSource == null ? "pre-vote" : "transfer-pre-vote", header.Term);
            try
            {
                var reply = await _inner.PreVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);
                _counter.ObservePreVoteResult?.Invoke(header.GroupId, header.SenderNodeId, nodeId, header.Term, $"granted={reply.Granted}, replyTerm={reply.CurrentTerm}, refusal={reply.RefusalCode}");
                return reply;
            }
            catch (Exception exception) when (_counter.ObservePreVoteResult != null)
            {
                _counter.ObservePreVoteResult(header.GroupId, header.SenderNodeId, nodeId, header.Term, $"{exception.GetType().Name}: {exception.Message}");
                throw;
            }
            finally
            {
                _counter.CountPreVote(header.GroupId, header.SenderNodeId, nodeId);
            }
        }

        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
        {
            _counter.Observe?.Invoke(header.GroupId, header.SenderNodeId, nodeId, "vote", header.Term);
            return _inner.RequestVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);
        }

        private Task SendHintAsync(string target, in ReplicaRpcHeader header, CancellationToken cancellationToken)
        {
            _counter.Observe?.Invoke(header.GroupId, header.SenderNodeId, target, "hint", header.Term);
            return _counter.DropHints ? Task.CompletedTask : _inner.SendTransferAsync(target, header, cancellationToken);
        }
    }
}
