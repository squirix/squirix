using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Watches which nodes of a cluster hold authority over one replica group, and fails as soon as two nodes ever held it in one term; every
/// wait it runs checks that on each poll.
/// </summary>
/// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
/// <remarks>
/// Election safety allows at most one leader per term over the whole run, not only at one instant: every observation is checked against
/// all earlier ones. Nodes of the topology that do not run, or are stopping, are skipped, so a test may stop and restart nodes between
/// waits. Observations are serialized, so several waits may run on one ledger at once.
/// </remarks>
[ThreadSafe]
internal sealed class GroupAuthorityLedger<TOptions>
    where TOptions : ClusterStartOptions
{
    private readonly TestCluster<TOptions> _cluster;
    private readonly Lock _gate = new();
    private readonly string _groupId;
    private readonly Dictionary<ulong, string> _holders = [];
    private readonly string[] _nodes;

    /// <summary>Initializes a new instance of the <see cref="GroupAuthorityLedger{TOptions}" /> class.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="groupId">The replica group watched.</param>
    /// <param name="bound">The longest wait of each wait this ledger runs; ninety seconds when not set.</param>
    /// <exception cref="ArgumentNullException"><paramref name="cluster" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException"><paramref name="groupId" /> is null, empty or white space.</exception>
    internal GroupAuthorityLedger(TestCluster<TOptions> cluster, string groupId, TimeSpan? bound = null)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _cluster = cluster;
        _groupId = groupId;
        Bound = bound ?? TimeSpan.FromSeconds(90);
        _nodes = new string[cluster.Topology.Count];
        for (var i = 0; i < _nodes.Length; i++)
            _nodes[i] = cluster.Topology[i].NodeId;
    }

    /// <summary>Gets the longest wait of each wait this ledger runs.</summary>
    internal TimeSpan Bound { get; }

    /// <summary>Gets the number of terms in which some node held authority so far.</summary>
    internal int Terms
    {
        get
        {
            lock (_gate)
                return _holders.Count;
        }
    }

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
            _ = Observe();
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
    internal Task<(string NodeId, ulong Term)> LeaderAsync(IReadOnlyList<string> candidates, ulong above, string phase, CancellationToken cancellationToken) =>
        LeaderAsync(candidates, above, phase, Bound, cancellationToken);

    /// <summary>Waits until one of the given nodes holds authority over the group in a term above the given one.</summary>
    /// <param name="candidates">The nodes that may lead.</param>
    /// <param name="above">The term the leader must exceed.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="bound">The longest wait.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader and its term.</returns>
    internal async Task<(string NodeId, ulong Term)> LeaderAsync(IReadOnlyList<string> candidates, ulong above, string phase, TimeSpan bound, CancellationToken cancellationToken)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        await UntilAsync(
            () =>
            {
                leader = Observe(candidates);
                return leader.Term > above;
            },
            phase,
            bound,
            cancellationToken);
        return leader;
    }

    /// <summary>Reads which running nodes of the topology hold authority over the group now, and records them.</summary>
    /// <returns>The node holding authority in the highest term, and that term; a zero term when none holds it.</returns>
    /// <exception cref="InvalidOperationException">Two nodes held authority over the group in the same term.</exception>
    internal (string NodeId, ulong Term) Observe() => Observe(_nodes);

    /// <summary>Reads which of the given nodes hold authority over the group now, and records them; nodes that do not run or are stopping are skipped.</summary>
    /// <param name="nodes">The nodes to read.</param>
    /// <returns>The node holding authority in the highest term, and that term; a zero term when none holds it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="nodes" /> is <see langword="null" />.</exception>
    /// <exception cref="InvalidOperationException">Two nodes held authority over the group in the same term.</exception>
    internal (string NodeId, ulong Term) Observe(IReadOnlyList<string> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        var leader = (NodeId: string.Empty, Term: 0UL);
        lock (_gate)
        {
            for (var i = 0; i < nodes.Count; i++)
            {
                if (!LeaderTableReads.TryGetAuthority(_cluster, nodes[i], _groupId, out var term))
                    continue;

                if (!_holders.TryAdd(term, nodes[i]) && !string.Equals(_holders[term], nodes[i], StringComparison.Ordinal))
                    throw new InvalidOperationException($"Nodes {_holders[term]} and {nodes[i]} both held authority over group {_groupId} in term {term}.");

                if (term > leader.Term)
                    leader = (nodes[i], term);
            }
        }

        return leader;
    }

    /// <summary>Waits until a condition holds, checking election safety over every node on every poll.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the condition holds.</returns>
    /// <exception cref="TimeoutException">The condition did not hold within <see cref="Bound" />; the message names the phase.</exception>
    internal Task UntilAsync(Func<bool> condition, string phase, CancellationToken cancellationToken) => UntilAsync(condition, phase, Bound, cancellationToken);

    /// <summary>Waits until a condition holds, checking election safety over every node on every poll.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="bound">The longest wait.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the condition holds.</returns>
    /// <exception cref="TimeoutException">The condition did not hold within <paramref name="bound" />; the message names the phase.</exception>
    internal Task UntilAsync(Func<bool> condition, string phase, TimeSpan bound, CancellationToken cancellationToken) =>
        PhaseAsync(
            phase,
            () => (Ledger: this, Condition: condition).WaitUntilAsync(
                static wait =>
                {
                    _ = wait.Ledger.Observe();
                    return wait.Condition();
                },
                bound,
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
                    _ = wait.Ledger.Observe();
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
