using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Mappers;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// With automatic failover on, links cut at the socket level split the group of one owner: the side with a majority elects a leader and
/// keeps committing, the other side fails closed, and no two nodes ever hold authority over the group in one term.
/// </summary>
/// <remarks>
/// Every wait reads the leader table of every node and checks election safety against all earlier reads. Writes go through the committers
/// of a node with authority: a client write reaches the new leader of a group only through leader routing, which this release does not
/// have yet, and reads are not fenced by authority yet either, so the minority is shown to fail closed on the write side.
/// </remarks>
public sealed class PartitionSafetyTests : NodeIntegrationTestBase
{
    private const string Group = "node-a";
    private const string Scope = "partition-safety";

    /// <summary>Bounds every wait; the timing below elects within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly ElectionTimerOptions Timing = new()
    {
        ElectionTimeout = TimeSpan.FromSeconds(2),
        HeartbeatInterval = TimeSpan.FromMilliseconds(200),
        MaxJitter = TimeSpan.FromSeconds(2),
        VoteRpcTimeout = TimeSpan.FromSeconds(1),
    };

    /// <summary>How long a negative is watched: three election rounds of the longest timeout, so any election that could run would have.</summary>
    private static readonly TimeSpan Watch = (Timing.ElectionTimeout + Timing.MaxJitter) * 3;

    private static readonly string[] Three = ["node-a", "node-b", "node-c"];

    /// <summary>A leader cut off from both followers loses its authority and refuses writes, while the two followers elect a leader that commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MajorityContinuesAndMinorityFailsClosed(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Three, Options("partition-majority", fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (former, formerTerm) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);
        var key = cluster[former].FindKeyOwnedBy(Scope, Group);
        await CommitAsync(ledger, cluster[former], key, cancellationToken);

        await fabric.IsolateAsync(former);
        var (next, nextTerm) = await ledger.LeaderAsync(Without(Three, former), formerTerm, "the majority elects a leader", cancellationToken);
        await ledger.UntilAsync(() => !HasAuthority(cluster[former]), "the cut-off leader loses its authority", cancellationToken);
        var formerLast = (await Log(cluster[former]).GetStatusAsync(cancellationToken)).LastLogIndex;
        var refused = NodeExceptionAssert.For<RpcException>().Throws(Committers(cluster[former]), key, static (committers, k) => _ = committers.ForKey(Scope, k));
        await CommitAsync(ledger, cluster[next], key, cancellationToken);

        _ = await Assert.That(nextTerm).IsGreaterThan(formerTerm);
        _ = await Assert.That(IsDefinitePreAppendRefusal(refused)).IsTrue().Because($"the cut-off leader refuses before any append, got {refused.StatusCode}: {refused.Status.Detail}");
        _ = await Assert.That((await Log(cluster[former]).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(formerLast);
    }

    /// <summary>
    /// A leader cut off from the majority does not commit what the majority commits, and the authority its status reports refuses a read;
    /// reads themselves are not fenced by authority yet.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinorityCannotServeCurrentRead(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Three, Options("partition-minority-read", fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (former, formerTerm) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);

        await fabric.IsolateAsync(former);
        var (next, _) = await ledger.LeaderAsync(Without(Three, former), formerTerm, "the majority elects a leader", cancellationToken);
        await CommitAsync(ledger, cluster[next], cluster[next].FindKeyOwnedBy(Scope, Group), cancellationToken);
        await ledger.UntilAsync(() => !HasAuthority(cluster[former]), "the cut-off leader loses its authority", cancellationToken);
        var current = (await Log(cluster[next]).GetStatusAsync(cancellationToken)).CommitIndex;
        var stale = await StatusAsync(cluster[former], cancellationToken);
        var read = LeaderAuthorityGate.CheckRead(
            stale.ReplicaCount,
            stale.HasMajorityContact,
            stale.IsLeader,
            stale.CurrentTerm,
            stale.ObservedTerm,
            new LeaderReadState(true, current, stale.LastAppliedIndex));

        _ = await Assert.That(stale.CommitIndex).IsLessThan(current);
        _ = await Assert.That((stale.IsLeader, read.Allowed)).IsEqualTo((false, false));
        _ = await Assert.That(stale.Role).IsNotEqualTo(ReplicaElectionRole.AuthorizedLeader);
    }

    /// <summary>A leader cut off while the majority elected a later term follows that term once the links heal, without authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FormerLeaderStepsDownOnHigherTerm(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Three, Options("partition-former-leader", fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (former, formerTerm) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);

        await fabric.IsolateAsync(former);
        var (next, nextTerm) = await ledger.LeaderAsync(Without(Three, former), formerTerm, "the majority elects a leader", cancellationToken);
        fabric.HealAll();
        await ledger.UntilValueAsync((Node: cluster[former], Leader: next, Term: nextTerm), static (s, token) => FollowsAsync(s.Node, s.Leader, s.Term, token), "the former leader follows the later term", cancellationToken);

        _ = await Assert.That(HasAuthority(cluster[former])).IsFalse();
        _ = await Assert.That(cluster[former].GetRequiredService<ReplicaGroupRegistry>().StateFor(Group).Role).IsEqualTo(ReplicaGroupRole.Follower);
        _ = await Assert.That(ledger.Observe(Three)).IsEqualTo((next, nextTerm));
    }

    /// <summary>Leader after leader is cut off and returns: every term has at most one node with authority, at any time of the run.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoTwoLeadersHoldAuthorityInOneTerm(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Three, Options("partition-one-leader", fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var leader = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);

        for (var round = 0; round < 3; round++)
        {
            await fabric.IsolateAsync(leader.NodeId);
            var next = await ledger.LeaderAsync(Without(Three, leader.NodeId), leader.Term, "the majority elects the next leader", cancellationToken);
            fabric.HealAll();
            await ledger.UntilValueAsync((Node: cluster[leader.NodeId], Leader: next.NodeId, next.Term), static (s, token) => FollowsAsync(s.Node, s.Leader, s.Term, token), "the former leader follows the next term", cancellationToken);
            leader = next;
        }

        _ = await Assert.That(ledger.Terms).IsGreaterThanOrEqualTo(4);
    }

    /// <summary>A group of four split two against two has no majority on either side: nobody holds authority until the links heal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfFourSplitHasNoLeader(CancellationToken cancellationToken)
    {
        string[] four = ["node-a", "node-b", "node-c", "node-d"];
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(four, Options("partition-rf4-split", fabric, 4), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (leader, term) = await ledger.LeaderAsync(four, 0UL, "the group gets a leader", cancellationToken);
        var others = Without(four, leader);

        await SplitAsync(fabric, [leader, others[0]], [others[1], others[2]]);
        await ledger.UntilAsync(() => ledger.Observe(four).Term == 0UL, "the leader of the split group loses its authority", cancellationToken);
        await ledger.HoldsAsync(() => ledger.Observe(four).Term == 0UL, Watch, "no node of the split group holds authority", cancellationToken);
        fabric.HealAll();
        var (_, healed) = await ledger.LeaderAsync(four, term, "the healed group elects a leader", cancellationToken);

        _ = await Assert.That(healed).IsGreaterThan(term);
    }

    /// <summary>A group of five whose leader is cut off with one follower: the other three elect a leader and commit, the two never lead.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfFiveMajorityContinuesAgainstMinority(CancellationToken cancellationToken)
    {
        string[] five = ["node-a", "node-b", "node-c", "node-d", "node-e"];
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(five, Options("partition-rf5", fabric, 5), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (former, formerTerm) = await ledger.LeaderAsync(five, 0UL, "the group gets a leader", cancellationToken);
        var others = Without(five, former);
        string[] minority = [former, others[0]];
        string[] majority = [others[1], others[2], others[3]];

        await SplitAsync(fabric, minority, majority);
        var (next, nextTerm) = await ledger.LeaderAsync(majority, formerTerm, "the three-node majority elects a leader", cancellationToken);
        await CommitAsync(ledger, cluster[next], cluster[next].FindKeyOwnedBy(Scope, Group), cancellationToken);
        await ledger.UntilAsync(() => ledger.Observe(minority).Term == 0UL, "the two-node minority holds no authority", cancellationToken);
        await ledger.HoldsAsync(() => ledger.Observe(minority).Term == 0UL, Watch, "the two-node minority never leads", cancellationToken);

        _ = await Assert.That(nextTerm).IsGreaterThan(formerTerm);
        _ = await Assert.That(ledger.Observe(five)).IsEqualTo((next, nextTerm));
    }

    /// <summary>
    /// A follower cut off from the leader alone campaigns in vain: the other follower still hears the leader and refuses its pre-votes, so
    /// the leader keeps its authority and no term moves.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IsolatedFollowerDoesNotDisrupt(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Three, Options("partition-follower", fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (leader, term) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);

        // A leader authorized by one follower while the other still starts may lose its quorum check to that start, not to the cut.
        var eligibility = cluster[leader].GetRequiredService<ReplicaGroupRegistry>().EligibilityFor(Group);
        await ledger.UntilAsync(eligibility.AllCanCountInWriteQuorum, "every follower counts in the write quorum", cancellationToken);
        await fabric.PartitionAsync(leader, Without(Three, leader)[0]);
        await ledger.HoldsAsync(() => ledger.Observe(Three) == (leader, term), Watch, "the leader keeps its authority in its term", cancellationToken);

        foreach (var node in Three)
            _ = await Assert.That((await Log(cluster[node]).GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(term);
    }

    /// <summary>
    /// A follower cut off while the leader commits is repaired as soon as it answers again: with the readiness poll set to an hour, only
    /// its answer to the leader brings it back into the write quorum.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReturnedFollowerIsRepairedWithoutThePoll(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        var options = Options("partition-returned-follower", fabric, 3, static services => services.AddSingleton(new ReplicaReadinessOptions { InitialDelay = TimeSpan.FromHours(1), MaxDelay = TimeSpan.FromHours(1) }));
        await using var cluster = await StartAsync(Three, options, cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (leader, _) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);
        var eligibility = cluster[leader].GetRequiredService<ReplicaGroupRegistry>().EligibilityFor(Group);
        await ledger.UntilAsync(eligibility.AllCanCountInWriteQuorum, "every follower counts in the write quorum", cancellationToken);
        var returned = Without(Three, leader)[0];
        var slot = SlotOf(cluster[leader], returned);

        await fabric.IsolateAsync(returned);
        await CommitAsync(ledger, cluster[leader], cluster[leader].FindKeyOwnedBy(Scope, Group), cancellationToken);
        await ledger.UntilAsync(() => !eligibility.CanCountInWriteQuorum(slot), "the cut-off follower leaves the write quorum", cancellationToken);
        var committed = (await Log(cluster[leader]).GetStatusAsync(cancellationToken)).CommitIndex;
        fabric.HealAll();
        await ledger.UntilAsync(() => eligibility.CanCountInWriteQuorum(slot), "the returned follower counts in the write quorum again", cancellationToken);

        _ = await Assert.That((await Log(cluster[returned]).GetStatusAsync(cancellationToken)).LastLogIndex).IsGreaterThanOrEqualTo(committed);
        _ = await Assert.That(ledger.Observe(Three).NodeId).IsEqualTo(leader);
    }

    /// <summary>Commits one write through the committers of a node with authority, retrying while the group is not ready to take it.</summary>
    /// <param name="ledger">The election safety record, checked on every attempt.</param>
    /// <param name="node">The node with authority.</param>
    /// <param name="key">A key of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the write committed.</returns>
    /// <remarks>Every attempt carries the same operation identifier, so a retry never writes twice.</remarks>
    private static Task CommitAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, string key, CancellationToken cancellationToken) =>
        ledger.UntilValueAsync(
            (Committers: Committers(node), Key: key, OperationId: Guid.NewGuid().ToString("N")),
            static async (write, token) =>
            {
                try
                {
                    await write.Committers.ForKey(Scope, write.Key).CommitSetAsync(write.OperationId, Scope, write.Key, new NodeCacheEntry<object?> { Value = write.OperationId }, token);
                    return true;
                }
                catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable or StatusCode.ResourceExhausted)
                {
                    return false;
                }
                catch (SquirixException)
                {
                    return false;
                }
            },
            "a write commits",
            cancellationToken);

    private static ReplicaGroupCommitters Committers(ITestNodeHost host) => host.GetRequiredService<ReplicaGroupCommitters>();

    /// <summary>Tells whether a node follows another leader of the group in a term at least the given one, without authority.</summary>
    /// <param name="node">The node.</param>
    /// <param name="leader">The leader it must follow.</param>
    /// <param name="term">The lowest term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> once it does.</returns>
    private static async ValueTask<bool> FollowsAsync(ITestNodeHost node, string leader, ulong term, CancellationToken cancellationToken)
    {
        var table = node.GetRequiredService<IGroupLeaderTable>();
        return (await Log(node).GetStatusAsync(cancellationToken)).CurrentTerm >= term && !table.HasLocalAuthority(Group, out _) &&
               table.TryGetLeader(Group, out var route) && string.Equals(route.NodeId, leader, StringComparison.Ordinal);
    }

    private static bool HasAuthority(ITestNodeHost node) => node.GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(Group, out _);

    /// <summary>
    /// Tells whether a refusal is definite and comes before any append: retryable while no leader is known, stale-term once a higher term
    /// was seen, or stale-owner once the contact of the new leader arrived. Which one depends on what the cut-off leader observed.
    /// </summary>
    /// <param name="refused">The refusal.</param>
    /// <returns><see langword="true" /> for one of the three pre-append refusals.</returns>
    private static bool IsDefinitePreAppendRefusal(RpcException refused) => (refused.StatusCode, refused.Status.Detail) switch
    {
        (StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail) => true,
        (StatusCode.FailedPrecondition, GrpcStaleOwnerMarkers.StaleTermErrorCodeValue) => true,
        (StatusCode.FailedPrecondition, _) => string.Equals(refused.Trailers.GetValue(GrpcStaleOwnerMarkers.ErrorCodeMetadataKey), "stale-owner", StringComparison.Ordinal),
        _ => false,
    };

    private static IFollowerLog Log(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Group, out var log) ? log : throw new InvalidOperationException($"The group log {Group} is not open.");

    private static IntegrationStartOptions Options(string scope, PartitionFabric fabric, int replicaCount = 3, Action<IServiceCollection>? configure = null) => new()
    {
        ReplicaCount = replicaCount,
        UsePersistence = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        PartitionFabric = fabric,
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(Timing);
            configure?.Invoke(services);
        },
    };

    private static int SlotOf(ITestNodeHost host, string nodeId)
    {
        var locator = host.GetRequiredService<IReplicaGroupLocator>();
        var members = new string[locator.ReplicaCount];
        locator.GetReplicaGroup(Group, members);
        return Array.IndexOf(members, nodeId);
    }

    /// <summary>Cuts every link between two sides.</summary>
    /// <param name="fabric">The partition fabric.</param>
    /// <param name="side">One side.</param>
    /// <param name="other">The other side.</param>
    /// <returns>A task that completes once every link between the sides is cut.</returns>
    private static async Task SplitAsync(PartitionFabric fabric, string[] side, string[] other)
    {
        foreach (var a in side)
            foreach (var b in other)
                await fabric.PartitionAsync(a, b);
    }

    private static async Task<ReplicaStatusSnapshot> StatusAsync(ITestNodeHost host, CancellationToken cancellationToken)
    {
        var snapshots = await host.GetRequiredService<IReplicaStatusSource>().GetSnapshotsAsync(cancellationToken);
        foreach (var snapshot in snapshots)
        {
            if (string.Equals(snapshot.GroupId, Group, StringComparison.Ordinal))
                return snapshot;
        }

        throw new InvalidOperationException($"The node reports no status of group {Group}.");
    }

    private static string[] Without(string[] nodes, string nodeId) => Array.FindAll(nodes, id => !string.Equals(id, nodeId, StringComparison.Ordinal));

    private ValueTask<TestCluster<IntegrationStartOptions>> StartAsync(string[] nodes, IntegrationStartOptions options, CancellationToken cancellationToken)
    {
        var topology = new ClusterNode[nodes.Length];
        for (var i = 0; i < nodes.Length; i++)
            topology[i] = new ClusterNode(nodes[i], GetNextHttpUri());

        return StartClusterAsync(topology, options, cancellationToken);
    }
}
