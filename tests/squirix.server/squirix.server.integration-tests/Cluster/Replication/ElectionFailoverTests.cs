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

/// <summary>With automatic failover on, the group of a stopped owner elects another leader, and the owner rejoins it as a follower.</summary>
public sealed class ElectionFailoverTests : NodeIntegrationTestBase
{
    private const string OwnerId = "node-a";

    /// <summary>Bounds every wait for the election; the timeouts below elect within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly string[] Survivors = ["node-b", "node-c"];

    /// <summary>
    /// The group of the owner gets a leader with authority, normally the owner itself in the provisional term; once that leader stops, a
    /// surviving node gains local authority over the group in a later term with its leader-term entry committed. The restarted former
    /// leader follows that term and holds no authority.
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
        await using var cluster = await StartClusterAsync(OwnerId, Survivors[0], Survivors[1], Options(scope, true), cancellationToken);
        var nodes = new[] { OwnerId, Survivors[0], Survivors[1] };
        var (firstLeader, firstTerm) = await LeaderAsync(cluster, nodes, "the group of the owner gets a leader", cancellationToken);

        await cluster.StopNodeAsync(firstLeader);
        var others = Array.FindAll(nodes, id => !string.Equals(id, firstLeader, StringComparison.Ordinal));
        var (secondLeader, elected) = await LeaderAsync(cluster, others, "a surviving node gains authority over the group", cancellationToken);
        var (noop, committed) = await LastEntryAsync(cluster[secondLeader], cancellationToken);

        var restarted = await cluster.StartNodeAsync(firstLeader, Options(scope, false), cancellationToken);
        await PhaseAsync(
            "the restarted former leader follows the new term",
            () => restarted.WaitUntilValueAsync((node, token) => FollowsAsync(node, (firstLeader, elected), token), Bound, cancellationToken));

        _ = await Assert.That(elected).IsGreaterThan(firstTerm);
        _ = await Assert.That(elected).IsGreaterThanOrEqualTo(2UL);
        _ = await Assert.That((noop.MutationKind, noop.Term, committed)).IsEqualTo((ReplicaMutationKinds.LeaderNoop, elected, true));
        _ = await Assert.That(Table(restarted).HasLocalAuthority(OwnerId, out _)).IsFalse();
    }

    /// <summary>Without automatic failover no election runs: the owner leads its group statically, as before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlagOffRegistersNoElection(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode(OwnerId, GetNextHttpUri()), new ClusterNode(Survivors[0], GetNextHttpUri()), new ClusterNode(Survivors[1], GetNextHttpUri())]);
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, ExtraScope = "election-flag-off" };
        var host = await cluster.StartNodeAsync(OwnerId, options, cancellationToken);

        var electionRegistered = false;
        foreach (var service in host.GetRequiredService<IEnumerable<IHostedService>>())
            electionRegistered |= service is ReplicaElectionService;

        _ = await Assert.That(electionRegistered).IsFalse();
        _ = await Assert.That(host.GetRequiredService<ReplicaGroupCommitters>().Leads(OwnerId)).IsTrue();
        _ = await Assert.That(host.GetRequiredService<ReplicaGroupRegistry>().StateFor(OwnerId).IsElectionDriven).IsFalse();
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

    /// <summary>Waits until one of the given nodes has local authority over the group of the owner.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="candidates">The nodes that may lead.</param>
    /// <param name="phase">What the wait expects, for the timeout message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader and its term.</returns>
    private static async Task<(string NodeId, ulong Term)> LeaderAsync(TestCluster<IntegrationStartOptions> cluster, string[] candidates, string phase, CancellationToken cancellationToken)
    {
        var leader = (NodeId: string.Empty, Term: 0UL);
        await PhaseAsync(
            phase,
            () => cluster.WaitUntilAsync(
                nodes =>
                {
                    foreach (var candidate in candidates)
                    {
                        if (Table(nodes[candidate]).HasLocalAuthority(OwnerId, out var term))
                            leader = (candidate, term);
                    }

                    return leader.Term != 0;
                },
                Bound,
                cancellationToken));
        return leader;
    }

    /// <summary>Tells whether a node follows the group of the owner in a term at least the given one, knowing another leader.</summary>
    /// <param name="node">The node.</param>
    /// <param name="follower">The identifier of the node, and the lowest term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> once it does.</returns>
    private static async ValueTask<bool> FollowsAsync(ITestNodeHost node, (string NodeId, ulong Term) follower, CancellationToken cancellationToken) =>
        (await Log(node).GetStatusAsync(cancellationToken)).CurrentTerm >= follower.Term && Table(node).TryGetLeader(OwnerId, out var route) &&
        !string.Equals(route.NodeId, follower.NodeId, StringComparison.Ordinal);

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

    private static IFollowerLog Log(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log : throw new InvalidOperationException($"The group log {OwnerId} is not open.");

    /// <summary>Reads the last entry of the owner group log on a node, and whether it is committed.</summary>
    /// <param name="host">The node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The decoded record and whether the commit index reaches it.</returns>
    /// <exception cref="InvalidOperationException">The log holds no decodable last entry.</exception>
    private static async Task<(ReplicaLogRecord Record, bool Committed)> LastEntryAsync(ITestNodeHost host, CancellationToken cancellationToken)
    {
        var log = Log(host);
        var status = await log.GetStatusAsync(cancellationToken);
        var read = await log.ReadEntriesAsync(status.LastLogIndex, 1, cancellationToken);
        var record = read.Entries.Count == 1 ? ReplicaLogCodec.Decode(read.Entries[0].Payload) : null;
        return record is { } last ? (last, status.CommitIndex >= status.LastLogIndex)
            : throw new InvalidOperationException($"The group log {OwnerId} holds no decodable entry at {status.LastLogIndex}.");
    }
}
