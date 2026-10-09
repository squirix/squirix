using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// An RF=5 group split into two nodes holding its leader and three others fails closed on the minority side: the two nodes serve no current
/// read and acknowledge no write, while the three elect a leader in a later term and keep serving through the SDK. Once the links heal, the
/// former leader follows the majority, no acknowledged write is lost, reads stay linearizable, and the five members retain one committed log.
/// </summary>
public sealed class FiveNodeMinorityTests : EndToEndTestBase
{
    private const string CacheName = "five-node-minority";

    /// <summary>The group under test, named after its owner; with five replicas every node is a member.</summary>
    private const string Group = "nodeA";

    /// <summary>The value of a register the readers must see before the fault starts, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    /// <summary>The writes, and the reads, each register runs after the recovery probe succeeded, so the workload covers the recovered group.</summary>
    private const int TailOperations = 10;

    /// <summary>
    /// The leader and one follower are cut off from the three other members under a register workload through the three. Clients of both
    /// cut-off nodes get no acknowledged write and no current read, right after the cut and once the three lead; after the links heal, the
    /// former leader follows the new one, serves what the three committed, and the writes it was refused are gone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(180_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task TwoNodeMinorityFailsClosed(CancellationToken cancellationToken)
    {
        const string testName = nameof(TwoNodeMinorityFailsClosed);
        await using var fabric = new PartitionFabric();
        await using var cluster = await HostedCluster.StartFiveNodeAsync(testName, FailoverSteps.Options(testName, fabric: fabric, replicaCount: 5), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.FiveNodes, FailoverSteps.Bound, cancellationToken);
        var others = FailoverSteps.Except(FailoverSteps.FiveNodes, former);
        string[] minority = [former, others[0]];
        var majority = FailoverSteps.Except(others, others[0]);
        var cutLeader = await cluster.GetCacheAsync<long>(CacheName, former, cancellationToken);
        var cutFollower = await cluster.GetCacheAsync<long>(CacheName, minority[1], cancellationToken);
        var writer = await cluster.GetCacheAsync<long>(CacheName, majority[0], cancellationToken);
        var reader = await cluster.GetCacheAsync<long>(CacheName, majority[1], cancellationToken);
        var keys = FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, Group, "register", 4);
        var singles = (
            Stale: FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, Group, "stale", 1)[0],
            LostByLeader: FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, Group, "lost-leader", 1)[0],
            LostByFollower: FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, Group, "lost-follower", 1)[0],
            Probe: FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, Group, "probe", 1)[0]);
        await cutLeader.SetAsync(singles.Stale, 1L, cancellationToken: cancellationToken);

        var registers = new RegisterWorkload(writer, reader, keys);
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = registers.RunUntilAsync(recovered.Task, TailOperations, cancellationToken);
        var safety = probe.Ledger(Group).UntilAsync(() => running.IsCompleted, "the workload ends", cancellationToken);
        try
        {
            await probe.Ledger(Group).UntilValueAsync(
                (Reader: reader, Key: keys[0]),
                static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
                "the workload makes progress",
                cancellationToken);

            var split = await SplitAsync((fabric, probe, (former, formerTerm), minority, majority), (cutLeader, cutFollower, writer), singles, recovered, cancellationToken);
            await running;
            await safety;

            var healed = await HealAsync(cluster, (fabric, probe), (cutLeader, cutFollower), (registers.History, keys, singles), split.Attempts, cancellationToken);
            await AssertOutcomeAsync((split, healed), (majority, formerTerm), registers.History);
        }
        catch
        {
            // Ends the workload, and waits for every started task, so none outlives the cluster; the first failure is the one rethrown.
            _ = recovered.TrySetResult();
            await Task.WhenAll(running, safety).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
    }

    /// <summary>Asserts that the minority failed closed, that the majority led and served, and that the healed cluster kept every acknowledged write.</summary>
    /// <param name="outcome">What the split and the healed cluster recorded.</param>
    /// <param name="expected">The members of the majority, and the term of the former leader.</param>
    /// <param name="history">The history of the workload.</param>
    /// <returns>A task that represents the asynchronous assertions.</returns>
    private static async Task AssertOutcomeAsync((Split Split, Healed Healed) outcome, (string[] Majority, ulong FormerTerm) expected, RegisterHistory history)
    {
        var (split, healed) = outcome;
        var (majority, formerTerm) = expected;
        var dump = Eventually.Dump(split.Attempts);
        _ = await Assert.That(IsRefusal(split.LeaderWrite)).IsTrue().Because($"the cut-off leader must refuse the write, got {Describe(split.LeaderWrite)}");
        _ = await Assert.That(IsRefusal(split.FollowerWrite)).IsTrue().Because($"the cut-off follower must refuse the write, got {Describe(split.FollowerWrite)}");
        _ = await Assert.That(split.HeldAuthority).IsTrue().Because("the cut-off leader must still believe it leads when the calls are issued, or they do not cover that window");
        _ = await Assert.That(IsRefusal(split.LeaderRead)).IsTrue().Because($"the cut-off leader must refuse the read, got {Describe(split.LeaderRead)}");
        _ = await Assert.That(IsRefusal(split.FollowerRead)).IsTrue().Because($"the cut-off follower must refuse the read, got {Describe(split.FollowerRead)}");
        _ = await Assert.That(IsRefusal(split.DeposedLeaderRead)).IsTrue().Because($"the deposed leader must refuse the read, got {Describe(split.DeposedLeaderRead)}");
        _ = await Assert.That(IsRefusal(split.DeposedFollowerRead)).IsTrue().Because($"the cut-off follower must refuse the read, got {Describe(split.DeposedFollowerRead)}");
        _ = await Assert.That(split.ProbeRead).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
        _ = await Assert.That(split.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump);
        _ = await Assert.That(majority).Contains(split.Leader.NodeId);
        _ = await Assert.That(split.Leader.Term).IsGreaterThan(formerTerm);
        _ = await Assert.That(majority).Contains(healed.Leader.NodeId);
        _ = await Assert.That(healed.Leader.Term).IsGreaterThanOrEqualTo(split.Leader.Term);
        _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
        _ = await Assert.That(healed.Lost.ByLeader).IsEqualTo(new CacheValueResult<long>(false, 0L)).Because("a write the cut-off leader refused must not surface after the links heal");
        _ = await Assert.That(healed.Lost.ByFollower).IsEqualTo(new CacheValueResult<long>(false, 0L)).Because("a write the cut-off follower refused must not surface after the links heal");
        _ = await Assert.That(healed.Stale.ByLeader).IsEqualTo(new CacheValueResult<long>(true, 2L)).Because(dump);
        _ = await Assert.That(healed.Stale.ByFollower).IsEqualTo(new CacheValueResult<long>(true, 2L)).Because(dump);
        _ = await Assert.That(healed.ClientEntries).IsGreaterThan(0);
    }

    /// <summary>
    /// Heals the links and waits until the five members follow one leader, reads every register through the former leader, reads the keys of
    /// the refused and the overwritten writes through both cut-off nodes, and audits the committed logs.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="infrastructure">The fabric and the leader probe.</param>
    /// <param name="caches">The caches of the former leader and of the cut-off follower.</param>
    /// <param name="checks">The history of the workload, its register keys, and the single keys.</param>
    /// <param name="attempts">Receives one entry per attempt of the reads.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the healed cluster reports.</returns>
    private static async Task<Healed> HealAsync(
        HostedCluster cluster,
        (PartitionFabric Fabric, ClusterLeaderProbe<ClusterStartOptions> Probe) infrastructure,
        (ICache<long> Leader, ICache<long> Follower) caches,
        (RegisterHistory History, string[] Keys, (string Stale, string LostByLeader, string LostByFollower, string Probe) Singles) checks,
        List<EventualAttempt> attempts,
        CancellationToken cancellationToken)
    {
        infrastructure.Fabric.HealAll();
        var leader = await infrastructure.Probe.WaitForStableLeaderAsync(Group, FailoverSteps.FiveNodes, FailoverSteps.Bound, cancellationToken);
        await FailoverSteps.ReadFinalAsync(checks.History, caches.Leader, checks.Keys, cancellationToken);
        var lostByLeader = await Eventually.SucceedsAsync((Cache: caches.Leader, Key: checks.Singles.LostByLeader), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var lostByFollower = await Eventually.SucceedsAsync((Cache: caches.Follower, Key: checks.Singles.LostByFollower), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var staleByLeader = await Eventually.SucceedsAsync((Cache: caches.Leader, Key: checks.Singles.Stale), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var staleByFollower = await Eventually.SucceedsAsync((Cache: caches.Follower, Key: checks.Singles.Stale), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.FiveNodes, FailoverSteps.Bound, cancellationToken);
        return new Healed(leader, (lostByLeader, lostByFollower), (staleByLeader, staleByFollower), report.ClientEntries);
    }

    private static string Describe(Exception exception) => exception is RpcException rpc ? $"{nameof(RpcException)} {rpc.StatusCode} '{rpc.Status.Detail}'" : exception.GetType().Name;

    /// <summary>Tells whether a failure is a refusal the client can act on: an unavailable, timed out or shed call, or a commit with an unknown outcome.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="true" /> for a refusal; otherwise <see langword="false" />.</returns>
    private static bool IsRefusal(Exception exception) =>
        exception is CommitOutcomeUnknownException or RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.ResourceExhausted };

    /// <summary>
    /// Cuts every link between the two sides, probes both cut-off nodes right after the cut and once the majority leads, waits for the
    /// majority to serve a write and a read, and then lets the workload finish its tail.
    /// </summary>
    /// <param name="scene">The fabric, the leader probe, the former leader, and the two sides.</param>
    /// <param name="caches">The caches of the cut-off leader and follower, and the cache the majority writes through.</param>
    /// <param name="singles">The keys of the stale read, of the two refused writes and of the recovery probe.</param>
    /// <param name="recovered">Completed once the majority serves, so the workload ends after its tail.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the split recorded.</returns>
    private static async Task<Split> SplitAsync(
        (PartitionFabric Fabric, ClusterLeaderProbe<ClusterStartOptions> Probe, (string NodeId, ulong Term) Former, string[] Minority, string[] Majority) scene,
        (ICache<long> Leader, ICache<long> Follower, ICache<long> Writer) caches,
        (string Stale, string LostByLeader, string LostByFollower, string Probe) singles,
        TaskCompletionSource recovered,
        CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var cuts = new List<Task>();
        foreach (var inside in scene.Minority)
            foreach (var outside in scene.Majority)
                cuts.Add(scene.Fabric.PartitionAsync(inside, outside));

        await Task.WhenAll(cuts);
        var attempts = new List<EventualAttempt>();
        var recovery = FailoverSteps.RecoverAsync(caches.Writer, singles.Probe, 1L, started, attempts, cancellationToken);

        // The cut-off leader may still hold authority right after the cut, so it must refuse by itself: no quorum confirms its read or commits
        // its write. Every call starts before any is awaited, so the reads are issued while the leader can still believe it leads.
        var (holder, _) = scene.Probe.Ledger(Group).Observe([scene.Former.NodeId]);
        var leaderWriting = caches.Leader.SetAsync(singles.LostByLeader, 99L, cancellationToken: cancellationToken);
        var followerWriting = caches.Follower.SetAsync(singles.LostByFollower, 98L, cancellationToken: cancellationToken);
        var leaderReading = caches.Leader.GetValueAsync(singles.Stale, cancellationToken);
        var followerReading = caches.Follower.GetValueAsync(singles.Stale, cancellationToken);
        var leaderWrite = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(leaderWriting);
        var followerWrite = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(followerWriting);
        var leaderRead = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(leaderReading);
        var followerRead = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(followerReading);
        var heldAuthority = string.Equals(holder, scene.Former.NodeId, StringComparison.Ordinal);
        var (probeRead, elapsed) = await recovery;
        var leader = await scene.Probe.WaitForNewLeaderAsync(Group, scene.Former.Term, FailoverSteps.Bound, cancellationToken);

        await Eventually.SucceedsAsync(
            (Cache: caches.Writer, Key: singles.Stale),
            static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token),
            FailoverSteps.Bound,
            attempts,
            cancellationToken);
        var deposedLeaderRead = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(caches.Leader.GetValueAsync(singles.Stale, cancellationToken));
        var deposedFollowerRead = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(caches.Follower.GetValueAsync(singles.Stale, cancellationToken));
        recovered.SetResult();
        return new Split((leaderWrite, followerWrite), (leaderRead, followerRead), (deposedLeaderRead, deposedFollowerRead), heldAuthority, (probeRead, elapsed), leader, attempts);
    }

    /// <summary>What the healed cluster reports.</summary>
    /// <param name="Leader">The leader the five members follow, and its term.</param>
    /// <param name="Lost">The values of the keys whose writes the cut-off leader and follower refused, each read through its own node.</param>
    /// <param name="Stale">The values of the key the majority overwrote, read through the former leader and through the cut-off follower.</param>
    /// <param name="ClientEntries">The number of client entries in the committed log the members share.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Healed(
        (string NodeId, ulong Term) Leader,
        (CacheValueResult<long> ByLeader, CacheValueResult<long> ByFollower) Lost,
        (CacheValueResult<long> ByLeader, CacheValueResult<long> ByFollower) Stale,
        int ClientEntries);

    /// <summary>What the split recorded.</summary>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Split
    {
        /// <summary>Initializes a new instance of the <see cref="Split" /> struct.</summary>
        /// <param name="writes">How the cut-off leader and follower refused their writes.</param>
        /// <param name="reads">How the cut-off leader and follower refused their reads right after the cut.</param>
        /// <param name="deposed">How the cut-off leader and follower refused a read once the majority led.</param>
        /// <param name="heldAuthority">Whether the cut-off leader still held authority when the calls were issued.</param>
        /// <param name="probe">The value the recovery probe read back through the majority, and the time from the cut until then.</param>
        /// <param name="leader">The leader the majority elected, and its term.</param>
        /// <param name="attempts">The attempts of the probe and of the later waits.</param>
        internal Split(
            (Exception Leader, Exception Follower) writes,
            (Exception Leader, Exception Follower) reads,
            (Exception Leader, Exception Follower) deposed,
            bool heldAuthority,
            (CacheValueResult<long> Read, TimeSpan Elapsed) probe,
            (string NodeId, ulong Term) leader,
            List<EventualAttempt> attempts)
        {
            LeaderWrite = writes.Leader;
            FollowerWrite = writes.Follower;
            LeaderRead = reads.Leader;
            FollowerRead = reads.Follower;
            DeposedLeaderRead = deposed.Leader;
            DeposedFollowerRead = deposed.Follower;
            HeldAuthority = heldAuthority;
            ProbeRead = probe.Read;
            Elapsed = probe.Elapsed;
            Leader = leader;
            Attempts = attempts;
        }

        /// <summary>Gets how the cut-off leader refused the write.</summary>
        internal Exception LeaderWrite { get; }

        /// <summary>Gets how the cut-off follower refused the write.</summary>
        internal Exception FollowerWrite { get; }

        /// <summary>Gets how the cut-off leader refused the read right after the cut.</summary>
        internal Exception LeaderRead { get; }

        /// <summary>Gets how the cut-off follower refused the read right after the cut.</summary>
        internal Exception FollowerRead { get; }

        /// <summary>Gets how the deposed leader refused a read once the majority led.</summary>
        internal Exception DeposedLeaderRead { get; }

        /// <summary>Gets how the cut-off follower refused a read once the majority led.</summary>
        internal Exception DeposedFollowerRead { get; }

        /// <summary>Gets a value indicating whether the cut-off leader still held authority when the calls were issued.</summary>
        internal bool HeldAuthority { get; }

        /// <summary>Gets the value the recovery probe read back through the majority.</summary>
        internal CacheValueResult<long> ProbeRead { get; }

        /// <summary>Gets the time from the cut until the probe read its value back.</summary>
        internal TimeSpan Elapsed { get; }

        /// <summary>Gets the leader the majority elected, and its term.</summary>
        internal (string NodeId, ulong Term) Leader { get; }

        /// <summary>Gets the attempts of the probe and of the later waits.</summary>
        internal List<EventualAttempt> Attempts { get; }
    }
}
