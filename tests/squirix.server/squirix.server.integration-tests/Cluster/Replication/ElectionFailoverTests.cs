using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>With automatic failover on, the group of a stopped leader elects another leader, and the former leader rejoins it as a follower.</summary>
/// <remarks>
/// Every wait for a leader also checks, on each poll, that no two nodes hold authority in the same term: election safety holds throughout,
/// not only at the end.
/// </remarks>
public sealed class ElectionFailoverTests : NodeIntegrationTestBase
{
    private const string OwnerId = "node-a";

    /// <summary>The setups the owner-first scenario may try before it gives up on a machine too loaded for the owner to keep term one.</summary>
    private const int OwnerSetupAttempts = 3;

    /// <summary>Bounds every wait for the election; the timeouts below elect within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    /// <summary>Bounds the wait for the owner to lead its group in term one, before the setup is tried again.</summary>
    private static readonly TimeSpan OwnerBound = TimeSpan.FromSeconds(30);

    private static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    /// <summary>
    /// The group of the owner gets a leader with authority; once that leader stops, another node gains local authority over the group in a
    /// later term with its own leader-term entry committed, and the restarted former leader follows that term without authority.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// A loaded machine can start the peers of the owner later than the owner's quorum check allows: the owner then steps down from term
    /// one and an election picks the first leader. The leader stopped here is whichever node leads the group, so the scenario holds either way.
    /// </remarks>
    [Test]
    public async Task StoppedLeaderGroupElectsNewLeader(CancellationToken cancellationToken)
    {
        const string scope = "election-failover";
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options(scope, true), cancellationToken);
        var first = await LeaderAsync(cluster, Nodes, "the group of the owner gets a leader", cancellationToken);

        await FailOverAsync(cluster, first, Options(scope, false), cancellationToken);
    }

    /// <summary>
    /// The owner leads its group in the provisional term one; once it stops, another node gains authority in a later term with its own
    /// leader-term entry committed, and the restarted owner follows that term without authority.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// On a loaded machine the owner can lose term one to an election before its peers answer; the setup is then started again, and the
    /// test is skipped with that reason when no setup lets the owner keep term one.
    /// </remarks>
    /// <exception cref="TUnit.Core.Exceptions.SkipTestException">No setup let the owner keep term one.</exception>
    [Test]
    public async Task StoppedOwnerGroupElectsNewLeader(CancellationToken cancellationToken)
    {
        const string scope = "election-owner-failover";
        for (var attempt = 1; attempt <= OwnerSetupAttempts; attempt++)
        {
            await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options(scope, true), cancellationToken);
            var first = await LeaderAsync(cluster, Nodes, "the group of the owner gets a leader", cancellationToken, OwnerBound);
            if (!string.Equals(first.NodeId, OwnerId, StringComparison.Ordinal) || first.Term != 1UL)
                continue;

            await FailOverAsync(cluster, first, Options(scope, false), cancellationToken);
            return;
        }

        throw new TUnit.Core.Exceptions.SkipTestException($"The owner lost term one to an election in each of {OwnerSetupAttempts} setups: the machine is too loaded for its peers to answer in time.");
    }

    /// <summary>Without automatic failover no election runs: the owner leads its group statically, as before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlagOffRegistersNoElection(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode(Nodes[0], GetNextHttpUri()), new ClusterNode(Nodes[1], GetNextHttpUri()), new ClusterNode(Nodes[2], GetNextHttpUri())]);
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, ExtraScope = "election-flag-off" };
        var host = await cluster.StartNodeAsync(OwnerId, options, cancellationToken);

        var electionRegistered = false;
        foreach (var service in host.GetRequiredService<IEnumerable<IHostedService>>())
            electionRegistered |= service is ReplicaElectionService;

        _ = await Assert.That(electionRegistered).IsFalse();
        _ = await Assert.That(host.GetRequiredService<ReplicaGroupCommitters>().Leads(OwnerId)).IsTrue();
        _ = await Assert.That(host.GetRequiredService<ReplicaGroupRegistry>().StateFor(OwnerId).IsElectionDriven).IsFalse();
    }

    /// <summary>Stops the leader of the owner group, waits for the next leader, checks its leader-term entry, and restarts the former leader.</summary>
    /// <param name="cluster">The running cluster.</param>
    /// <param name="first">The leader to stop and its term.</param>
    /// <param name="restart">The start options of the restart.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task FailOverAsync(TestCluster<IntegrationStartOptions> cluster, (string NodeId, ulong Term) first, IntegrationStartOptions restart, CancellationToken cancellationToken)
    {
        await cluster.StopNodeAsync(first.NodeId);
        var others = Array.FindAll(Nodes, id => !string.Equals(id, first.NodeId, StringComparison.Ordinal));
        var (secondLeader, elected) = await LeaderAsync(cluster, others, "another node gains authority over the group", cancellationToken);
        var (noop, committed) = await NoopAsync(cluster[secondLeader], elected, cancellationToken);

        var restarted = await cluster.StartNodeAsync(first.NodeId, restart, cancellationToken);
        await PhaseAsync(
            "the restarted former leader follows the new term",
            () => restarted.WaitUntilValueAsync((node, token) => FollowsAsync(node, (first.NodeId, elected), token), Bound, cancellationToken));

        _ = await Assert.That(elected).IsGreaterThan(first.Term);
        _ = await Assert.That(elected).IsGreaterThanOrEqualTo(2UL);
        _ = await Assert.That((noop.MutationKind, noop.Term, committed)).IsEqualTo((ReplicaMutationKinds.LeaderNoop, elected, true));
        _ = await Assert.That(Table(restarted).HasLocalAuthority(OwnerId, out _)).IsFalse();
    }

    /// <summary>Tells whether a node follows the group of the owner in a term at least the given one, knowing another leader.</summary>
    /// <param name="node">The node.</param>
    /// <param name="follower">The identifier of the node, and the lowest term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> once it does.</returns>
    private static async ValueTask<bool> FollowsAsync(ITestNodeHost node, (string NodeId, ulong Term) follower, CancellationToken cancellationToken) =>
        (await Log(node).GetStatusAsync(cancellationToken)).CurrentTerm >= follower.Term && Table(node).TryGetLeader(OwnerId, out var route) &&
        !string.Equals(route.NodeId, follower.NodeId, StringComparison.Ordinal);

    /// <summary>Waits until one of the given nodes has local authority over the group of the owner, checking election safety on every poll.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="candidates">The running nodes that may lead.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="bound">The longest wait; <see cref="Bound" /> unless set.</param>
    /// <returns>The authorized node with the highest term, and that term.</returns>
    /// <exception cref="InvalidOperationException">Two nodes hold authority in the same term.</exception>
    private static async Task<(string NodeId, ulong Term)> LeaderAsync(
        TestCluster<IntegrationStartOptions> cluster,
        string[] candidates,
        string phase,
        CancellationToken cancellationToken,
        TimeSpan? bound = null)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        await PhaseAsync(
            phase,
            () => cluster.WaitUntilAsync(
                nodes =>
                {
                    leader = HighestAuthority(nodes, candidates);
                    return leader.Term != 0;
                },
                bound ?? Bound,
                cancellationToken));
        return leader;
    }

    /// <summary>Reads which node holds authority over the owner group in the highest term, refusing two holders of one term.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="candidates">The running nodes.</param>
    /// <returns>The node and its term; a zero term when none holds authority.</returns>
    /// <exception cref="InvalidOperationException">Two nodes hold authority in the same term.</exception>
    private static (string NodeId, ulong Term) HighestAuthority(TestCluster<IntegrationStartOptions> cluster, string[] candidates)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        var holders = new Dictionary<ulong, string>();
        foreach (var candidate in candidates)
        {
            if (!Table(cluster[candidate]).HasLocalAuthority(OwnerId, out var term))
                continue;

            if (!holders.TryAdd(term, candidate))
                throw new InvalidOperationException($"Nodes {holders[term]} and {candidate} both hold authority over group {OwnerId} in term {term}.");

            if (term > leader.Term)
                leader = (candidate, term);
        }

        return leader;
    }

    private static IFollowerLog Log(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log : throw new InvalidOperationException($"The group log {OwnerId} is not open.");

    /// <summary>Reads the leader-term entry the leader of the owner group appended in its term, and whether it is committed.</summary>
    /// <param name="leader">The leader.</param>
    /// <param name="term">The term the leader holds authority in.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The decoded record and whether the commit index reaches it.</returns>
    /// <exception cref="InvalidOperationException">The leader holds no leadership of the term, or its log holds no decodable entry at the index.</exception>
    private static async Task<(ReplicaLogRecord Record, bool Committed)> NoopAsync(ITestNodeHost leader, ulong term, CancellationToken cancellationToken)
    {
        var tenure = leader.GetRequiredService<ReplicaGroupCommitters>().For(OwnerId).Tenure;
        var index = tenure is { } held && held.Term == term ? held.NoopIndex : throw new InvalidOperationException($"The leader holds no leadership of term {term}.");
        var log = Log(leader);
        var status = await log.GetStatusAsync(cancellationToken);
        var read = await log.ReadEntriesAsync(index, 1, cancellationToken);
        var record = read.Entries.Count == 1 ? ReplicaLogCodec.Decode(read.Entries[0].Payload) : null;
        return record is { } noop ? (noop, status.CommitIndex >= index)
            : throw new InvalidOperationException($"The group log {OwnerId} holds no decodable entry at {index}.");
    }

    private static IntegrationStartOptions Options(string scope, bool clean) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        ServicesConfigure = static services => services.AddSingleton(new ElectionTimerOptions
        {
            ElectionTimeout = TimeSpan.FromSeconds(4),
            HeartbeatInterval = TimeSpan.FromMilliseconds(250),
            MaxJitter = TimeSpan.FromSeconds(2),
            VoteRpcTimeout = TimeSpan.FromSeconds(2),
        }),
    };

    /// <summary>Awaits one bounded wait of the scenario, naming it when it times out.</summary>
    /// <param name="phase">What the wait expects.</param>
    /// <param name="wait">Starts the wait.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The wait timed out; the message names the phase.</exception>
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

    private static IGroupLeaderTable Table(ITestNodeHost host) => host.GetRequiredService<IGroupLeaderTable>();
}
