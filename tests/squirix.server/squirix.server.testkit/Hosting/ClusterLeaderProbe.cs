using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Finds the leaders of the replica groups of a test cluster at run time, so a test never assumes which node leads; every poll also checks
/// election safety through the <see cref="GroupAuthorityLedger{TOptions}" /> of the group.
/// </summary>
/// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
/// <remarks>Every wait is a bounded poll. Waits on different groups, or on one group, may run at once.</remarks>
[ThreadSafe]
internal sealed class ClusterLeaderProbe<TOptions>
    where TOptions : ClusterStartOptions
{
    private readonly TestCluster<TOptions> _cluster;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, GroupAuthorityLedger<TOptions>> _ledgers = [with(StringComparer.Ordinal)];
    private readonly string[] _nodeIds;

    /// <summary>Initializes a new instance of the <see cref="ClusterLeaderProbe{TOptions}" /> class.</summary>
    /// <param name="cluster">The cluster.</param>
    internal ClusterLeaderProbe(TestCluster<TOptions> cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        _cluster = cluster;
        _nodeIds = new string[cluster.Topology.Count];
        for (var i = 0; i < _nodeIds.Length; i++)
            _nodeIds[i] = cluster.Topology[i].NodeId;
    }

    /// <summary>Checks for a while that no election runs in a group: no running node sees a later term and the leader keeps its authority.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="watch">How long the group is watched.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the watch ended without an election.</returns>
    /// <exception cref="InvalidOperationException">
    /// No running node holds authority when the watch starts, a running node saw a later term, the leader changed, or two nodes held
    /// authority in one term.
    /// </exception>
    internal Task AssertNoElectionAsync(string groupId, TimeSpan watch, CancellationToken cancellationToken) => AssertNoElectionAsync(groupId, watch, true, cancellationToken);

    /// <summary>Checks for a while that no election runs in a group: no running node sees a later term and the leader keeps its authority.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="watch">How long the group is watched.</param>
    /// <param name="requireLeader">
    /// Whether a running node must hold authority when the watch starts; when <see langword="false" /> and none does, the watch checks that
    /// no node gains authority and no node sees a term above the highest one seen at the start.
    /// </param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the watch ended without an election.</returns>
    /// <exception cref="InvalidOperationException">
    /// A leader is required but none holds authority at the start, a running node saw a later term, the leader changed, or two nodes held
    /// authority in one term.
    /// </exception>
    internal async Task AssertNoElectionAsync(string groupId, TimeSpan watch, bool requireLeader, CancellationToken cancellationToken)
    {
        var ledger = Ledger(groupId);
        var leader = ledger.Observe();
        if (requireLeader && leader.Term == 0UL)
            throw new InvalidOperationException($"No running node holds authority over group {groupId} when the no-election watch starts.");

        var term = HighestTerm(groupId);
        await ledger.HoldsAsync(
            () => ledger.Observe() == leader && HighestTerm(groupId) == term,
            watch,
            $"group {groupId} keeps leader {leader.NodeId} in term {leader.Term} and no node sees a term above {term}",
            cancellationToken);
    }

    /// <summary>Gets the election safety record of a group, created on first use.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <returns>The ledger every wait of this probe for the group checks.</returns>
    internal GroupAuthorityLedger<TOptions> Ledger(string groupId)
    {
        lock (_gate)
        {
            if (!_ledgers.TryGetValue(groupId, out var ledger))
            {
                ledger = new GroupAuthorityLedger<TOptions>(_cluster, groupId);
                _ledgers.Add(groupId, ledger);
            }

            return ledger;
        }
    }

    /// <summary>Tells whether a running node is a member of a group: whether it holds election state for it.</summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="groupId">The replica group.</param>
    /// <returns><see langword="true" /> when the node runs and serves the group; <see langword="false" /> for a node outside the group, or one that does not run.</returns>
    internal bool Serves(string nodeId, string groupId) => LeaderTableReads.TryRead(_cluster, nodeId, groupId, out _);

    /// <summary>Reads the leader a running node outside a group learned for it, from the leader a peer named.</summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="leader">The learned leader and its term; the node identifier is empty when none is kept.</param>
    /// <returns><see langword="true" /> when the node runs and keeps a learned leader for the group.</returns>
    internal bool TryGetLearnedLeader(string nodeId, string groupId, out (string NodeId, ulong Term) leader)
    {
        var known = LeaderTableReads.TryGetLearned(_cluster, nodeId, groupId, out var route);
        leader = known ? (route.NodeId, route.Term) : (string.Empty, 0UL);
        return known;
    }

    /// <summary>Waits until a running node holds authority over a group in a term above the given one.</summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="aboveTerm">The term the new leader must exceed.</param>
    /// <param name="bound">The longest wait.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The new leader and its term.</returns>
    /// <exception cref="TimeoutException">No running node gained authority in a later term within the bound.</exception>
    internal Task<(string NodeId, ulong Term)> WaitForNewLeaderAsync(string groupId, ulong aboveTerm, TimeSpan bound, CancellationToken cancellationToken) =>
        Ledger(groupId).LeaderAsync(_nodeIds, aboveTerm, $"group {groupId} gets a leader in a term above {aboveTerm}", bound, cancellationToken);

    /// <summary>
    /// Waits until a group has a stable leader: one of the members holds authority, and every member runs, acts in that term and routes
    /// to that leader.
    /// </summary>
    /// <param name="groupId">The replica group.</param>
    /// <param name="members">The members of the group that must agree; each must run.</param>
    /// <param name="bound">The longest wait.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader and its term.</returns>
    /// <exception cref="TimeoutException">The members did not agree on one leader within the bound.</exception>
    internal async Task<(string NodeId, ulong Term)> WaitForStableLeaderAsync(string groupId, IReadOnlyList<string> members, TimeSpan bound, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(members);
        var ledger = Ledger(groupId);
        var leader = (NodeId: string.Empty, Term: 0UL);
        await ledger.UntilAsync(
            () =>
            {
                leader = ledger.Observe(members);
                return leader.Term != 0UL && AllFollow(groupId, members, new LeaderRoute(leader.NodeId, leader.Term));
            },
            $"every member of group {groupId} follows one leader",
            bound,
            cancellationToken);
        return leader;
    }

    private bool AllFollow(string groupId, IReadOnlyList<string> members, LeaderRoute leader)
    {
        for (var i = 0; i < members.Count; i++)
        {
            if (!LeaderTableReads.TryRead(_cluster, members[i], groupId, out var view) || view.Term != leader.Term || view.Known != leader)
                return false;
        }

        return true;
    }

    private ulong HighestTerm(string groupId)
    {
        var term = 0UL;
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            if (LeaderTableReads.TryRead(_cluster, _nodeIds[i], groupId, out var view))
                term = Math.Max(term, view.HighestObservedTerm);
        }

        return term;
    }
}
