using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Squirix.Server.Adapters.Grpc.Replication;
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
    internal void Register(IServiceCollection services)
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

        public async Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
        {
            try
            {
                return await _inner.PreVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);
            }
            finally
            {
                _counter.CountPreVote(header.GroupId, header.SenderNodeId, nodeId);
            }
        }

        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            _inner.RequestVoteAsync(nodeId, header, lastLogIndex, lastLogTerm, cancellationToken);
    }
}
