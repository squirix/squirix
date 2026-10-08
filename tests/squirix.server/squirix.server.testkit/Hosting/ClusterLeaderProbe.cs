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
/// <remarks>Every wait is a bounded poll. Used from one test at a time.</remarks>
[Mutable]
internal sealed class ClusterLeaderProbe<TOptions>
    where TOptions : ClusterStartOptions
{
    private readonly TestCluster<TOptions> _cluster;
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
    /// <exception cref="InvalidOperationException">A running node saw a later term, the leader changed, or two nodes held authority in one term.</exception>
    internal async Task AssertNoElectionAsync(string groupId, TimeSpan watch, CancellationToken cancellationToken)
    {
        var ledger = Ledger(groupId);
        var leader = ledger.Observe();
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
        if (!_ledgers.TryGetValue(groupId, out var ledger))
        {
            ledger = new GroupAuthorityLedger<TOptions>(_cluster, groupId);
            _ledgers.Add(groupId, ledger);
        }

        return ledger;
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
            if (!_cluster.TryGetNode(members[i], out var node))
                return false;

            var view = node.GetRequiredService<IGroupLeaderTable>().Read(groupId);
            if (view.Term != leader.Term || view.Known != leader)
                return false;
        }

        return true;
    }

    private ulong HighestTerm(string groupId)
    {
        var term = 0UL;
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            if (_cluster.TryGetNode(_nodeIds[i], out var node))
                term = Math.Max(term, node.GetRequiredService<IGroupLeaderTable>().Read(groupId).HighestObservedTerm);
        }

        return term;
    }
}
