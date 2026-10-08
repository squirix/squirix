using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Watches which nodes of a cluster hold authority over one replica group, and fails as soon as two nodes ever held it in one term; every
/// wait it runs checks that on each poll.
/// </summary>
/// <remarks>
/// Election safety allows at most one leader per term over the whole run, not only at one instant: every observation is checked against
/// all earlier ones. Used from one test at a time, over a cluster whose nodes all run.
/// </remarks>
[Mutable]
internal sealed class GroupAuthorityLedger
{
    private readonly TestCluster<IntegrationStartOptions> _cluster;
    private readonly string _groupId;
    private readonly Dictionary<ulong, string> _holders = [];
    private readonly string[] _nodes;

    /// <summary>Initializes a new instance of the <see cref="GroupAuthorityLedger" /> class.</summary>
    /// <param name="cluster">The cluster; every node of its topology runs.</param>
    /// <param name="groupId">The replica group watched.</param>
    /// <param name="bound">The longest wait of each wait this ledger runs.</param>
    internal GroupAuthorityLedger(TestCluster<IntegrationStartOptions> cluster, string groupId, TimeSpan bound)
    {
        _cluster = cluster;
        _groupId = groupId;
        Bound = bound;
        _nodes = new string[cluster.Topology.Count];
        for (var i = 0; i < _nodes.Length; i++)
            _nodes[i] = cluster.Topology[i].NodeId;
    }

    /// <summary>Gets the longest wait of each wait this ledger runs.</summary>
    internal TimeSpan Bound { get; }

    /// <summary>Gets the number of terms in which some node held authority so far.</summary>
    internal int Terms => _holders.Count;

    /// <summary>Checks that an invariant holds on every poll for a while, checking election safety over every node as well.</summary>
    /// <param name="invariant">The invariant.</param>
    /// <param name="watch">How long the invariant is watched.</param>
    /// <param name="what">What the invariant states, for the failure message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the watch ended with the invariant held throughout.</returns>
    /// <exception cref="InvalidOperationException">The invariant failed on some poll, or two nodes held authority in one term.</exception>
    internal async Task HoldsAsync(Func<bool> invariant, TimeSpan watch, string what, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + Convert.ToInt64(watch.TotalMilliseconds);
        while (Environment.TickCount64 < deadline)
        {
            _ = Observe(_nodes);
            if (!invariant())
                throw new InvalidOperationException($"Expected that {what} throughout the watch.");

            await Task.Delay(25, cancellationToken);
        }
    }

    /// <summary>Waits until one of the given nodes holds authority over the group in a term above the given one.</summary>
    /// <param name="candidates">The nodes that may lead.</param>
    /// <param name="above">The term the leader must exceed.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader and its term.</returns>
    internal async Task<(string NodeId, ulong Term)> LeaderAsync(IReadOnlyList<string> candidates, ulong above, string phase, CancellationToken cancellationToken)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        await UntilAsync(
            () =>
            {
                leader = Observe(candidates);
                return leader.Term > above;
            },
            phase,
            cancellationToken);
        return leader;
    }

    /// <summary>Reads which of the given nodes hold authority over the group now, and records them.</summary>
    /// <param name="nodes">The nodes to read.</param>
    /// <returns>The node holding authority in the highest term, and that term; a zero term when none holds it.</returns>
    /// <exception cref="InvalidOperationException">Two nodes held authority over the group in the same term.</exception>
    internal (string NodeId, ulong Term) Observe(IReadOnlyList<string> nodes)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        for (var i = 0; i < nodes.Count; i++)
        {
            if (!_cluster[nodes[i]].GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(_groupId, out var term))
                continue;

            if (!_holders.TryAdd(term, nodes[i]) && !string.Equals(_holders[term], nodes[i], StringComparison.Ordinal))
                throw new InvalidOperationException($"Nodes {_holders[term]} and {nodes[i]} both held authority over group {_groupId} in term {term}.");

            if (term > leader.Term)
                leader = (nodes[i], term);
        }

        return leader;
    }

    /// <summary>Waits until a condition holds, checking election safety over every node on every poll.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the condition holds.</returns>
    /// <exception cref="TimeoutException">The condition did not hold within <see cref="Bound" />; the message names the phase.</exception>
    internal Task UntilAsync(Func<bool> condition, string phase, CancellationToken cancellationToken) =>
        PhaseAsync(
            phase,
            () => (Ledger: this, Condition: condition).WaitUntilAsync(
                static wait =>
                {
                    _ = wait.Ledger.Observe(wait.Ledger._nodes);
                    return wait.Condition();
                },
                Bound,
                cancellationToken));

    /// <summary>Waits until an asynchronous condition holds, checking election safety over every node on every poll.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state the condition reads.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the condition holds.</returns>
    /// <exception cref="TimeoutException">The condition did not hold within <see cref="Bound" />; the message names the phase.</exception>
    internal Task UntilValueAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask<bool>> condition, string phase, CancellationToken cancellationToken) =>
        PhaseAsync(
            phase,
            () => (Ledger: this, State: state, Condition: condition).WaitUntilValueAsync(
                static (wait, token) =>
                {
                    _ = wait.Ledger.Observe(wait.Ledger._nodes);
                    return wait.Condition(wait.State, token);
                },
                Bound,
                cancellationToken));

    private static async Task PhaseAsync(string phase, Func<Task> wait)
    {
        try
        {
            await wait();
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException($"Timed out waiting until {phase}.", exception);
        }
    }
}
