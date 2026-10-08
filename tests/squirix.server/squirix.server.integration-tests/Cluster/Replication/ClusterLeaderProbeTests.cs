using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// On a three-node cluster with automatic failover on, the leader probe finds the leader at run time, sees an election as soon as one runs,
/// and the failover timeline records the phases of a leader change in order.
/// </summary>
public sealed class ClusterLeaderProbeTests : NodeIntegrationTestBase
{
    private const string Group = "node-a";

    /// <summary>Bounds every wait; the timing below elects within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly TestElectionTiming Timing = new() { JitterSeed = 7UL };

    private static readonly string[] Three = ["node-a", "node-b", "node-c"];

    /// <summary>The election timing of the start options reaches every node, each with its own jitter seed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElectionTimingReachesEveryNode(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("probe-timing"), cancellationToken);

        var seeds = new ulong[Three.Length];
        for (var i = 0; i < Three.Length; i++)
        {
            var options = cluster[Three[i]].GetRequiredService<ElectionTimerOptions>();
            seeds[i] = options.JitterSeed;
            _ = await Assert.That((options.ElectionTimeout, options.HeartbeatInterval, options.MaxJitter, options.VoteRpcTimeout))
                            .IsEqualTo((Timing.ElectionTimeout, Timing.HeartbeatInterval, Timing.MaxJitter, Timing.VoteRpcTimeout));
        }

        _ = await Assert.That(seeds[0] != seeds[1] && seeds[1] != seeds[2] && seeds[0] != seeds[2]).IsTrue();
    }

    /// <summary>The stable leader the probe finds is the leader every member routes to, in its term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StableLeaderIsFollowedByAll(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("probe-stable"), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);

        var (leader, term) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);

        _ = await Assert.That(Three).Contains(leader);
        foreach (var node in Three)
        {
            _ = await Assert.That(cluster[node].GetRequiredService<IGroupLeaderTable>().TryGetLeader(Group, out var route)).IsTrue();
            _ = await Assert.That(route).IsEqualTo(new LeaderRoute(leader, term));
        }
    }

    /// <summary>A group whose members all run keeps its leader and term over two election rounds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QuietGroupRunsNoElection(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("probe-quiet"), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var leader = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);

        await probe.AssertNoElectionAsync(Group, Timing.Round * 2, cancellationToken);

        _ = await Assert.That(probe.Ledger(Group).Observe()).IsEqualTo(leader);
    }

    /// <summary>The no-election watch fails as soon as the majority elects a new leader after the leader is cut off.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoElectionWatchSeesElection(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("probe-watch", fabric), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);

        var watch = probe.AssertNoElectionAsync(Group, Bound, cancellationToken);
        await fabric.IsolateAsync(leader);
        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(watch);

        _ = await Assert.That(failure.Message).Contains($"group {Group} keeps leader {leader}", StringComparison.Ordinal);
    }

    /// <summary>A stopped leader is replaced, and the timeline records the loss, the later term, the new leader and convergence in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedLeaderTimelineIsOrdered(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("probe-timeline"), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);

        var timeline = FailoverTimeline<IntegrationStartOptions>.Start(cluster, Group);
        await using (timeline)
        {
            timeline.Mark("the leader stops");
            await cluster.StopNodeAsync(former);
            var survivors = Array.FindAll(Three, id => !string.Equals(id, former, StringComparison.Ordinal));
            _ = await probe.WaitForNewLeaderAsync(Group, formerTerm, Bound, cancellationToken);
            _ = await probe.WaitForStableLeaderAsync(Group, survivors, Bound, cancellationToken);
            await timeline.WaitUntilAsync(static t => t[FailoverPhase.Converged] != null, Bound, cancellationToken);
        }

        var dump = timeline.Dump();
        var lost = timeline[FailoverPhase.LeaderLost] ?? TimeSpan.MaxValue;
        var raised = timeline[FailoverPhase.TermRaised] ?? TimeSpan.MaxValue;
        var elected = timeline[FailoverPhase.NewLeader] ?? TimeSpan.MaxValue;
        var converged = timeline[FailoverPhase.Converged] ?? TimeSpan.MaxValue;
        _ = await Assert.That(timeline.Baseline).IsEqualTo(new LeaderRoute(former, formerTerm)).Because(dump);
        _ = await Assert.That(timeline.NewLeader.Term).IsGreaterThan(formerTerm).Because(dump);
        _ = await Assert.That(lost <= elected && raised <= elected && elected <= converged).IsTrue().Because(dump);
    }

    /// <summary>Nodes whose wall clocks run thirty seconds apart still elect one leader that every member follows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkewedClocksStillElectLeader(CancellationToken cancellationToken)
    {
        TimeSpan[] offsets = [TimeSpan.FromSeconds(-30), TimeSpan.Zero, TimeSpan.FromSeconds(30)];
        await using var cluster = CreateCluster([new ClusterNode(Three[0], GetNextHttpUri()), new ClusterNode(Three[1], GetNextHttpUri()), new ClusterNode(Three[2], GetNextHttpUri())]);
        for (var i = 0; i < Three.Length; i++)
            _ = await cluster.StartNodeAsync(Three[i], Skewed(offsets[i]), cancellationToken);

        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);

        _ = await Assert.That(Three).Contains(leader);
        for (var i = 0; i < Three.Length; i++)
        {
            var clock = await Assert.That(cluster[Three[i]].GetRequiredService<TimeProvider>()).IsTypeOf<SkewedTimeProvider>();
            _ = await Assert.That(clock!.Offset).IsEqualTo(offsets[i]);
        }
    }

    private static IntegrationStartOptions Options(string scope, PartitionFabric? fabric = null) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        ElectionTiming = Timing,
        PartitionFabric = fabric,
    };

    private static IntegrationStartOptions Skewed(TimeSpan offset) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = "probe-skew",
        AutomaticFailoverEnabled = true,
        ElectionTiming = Timing,
        TimeProvider = new SkewedTimeProvider(offset),
    };
}
