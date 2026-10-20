using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Enums;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A follower that was down while its group was written, and whose group log the other members compacted meanwhile, is repaired once a
/// member that holds the compacted log leads the group; the repaired follower later leads, and a client reads every key through it.
/// </summary>
public sealed class StateTransferFailoverTests : EndToEndTestBase
{
    private const string CacheName = "state-transfer";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    private const int FirstKeys = 5;
    private const int LaterKeys = 45;

    /// <summary>The retained entries at which a group log is compacted, and the pause between two compaction passes.</summary>
    private static readonly (int Entries, TimeSpan Interval) Compaction = (16, TimeSpan.FromMilliseconds(100));

    /// <summary>
    /// A follower stops while the leader and the other follower write the group; the other follower compacts its log past the stopped one, the
    /// leader stops, and the returned follower is repaired by the member that now leads. Once the leader returns behind a cut link, the survivor
    /// stops: the repaired follower wins the election, because the returned leader lacks what it holds, and a client reads every key through it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(180_000)]
    [ParallelLimiter<FailoverLimit>]
    [ExecutionPriority(Priority.Critical)]
    public async Task RepairedFollowerLeadsAndServesEveryKey(CancellationToken cancellationToken)
    {
        const string testName = nameof(RepairedFollowerLeadsAndServesEveryKey);
        await using var fabric = new PartitionFabric();
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName, fabric: fabric, compaction: Compaction), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, term) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var others = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var (lagging, survivor) = (others[0], others[1]);
        var keys = FailoverSteps.KeysOf(CacheName, Group, "transfer", FirstKeys + LaterKeys + 1);
        var writer = await cluster.GetCacheAsync<long>(CacheName, survivor, cancellationToken);
        await LagBehindAsync(cluster, probe, (former, lagging, survivor), (writer, keys), cancellationToken);

        await cluster.StopNodeAsync(former);
        await cluster.RestartNodeAsync(lagging, cancellationToken);
        var (repairer, repairedTerm) = await probe.WaitForNewLeaderAsync(Group, term, FailoverSteps.Bound, cancellationToken);
        await WriteAsync(writer, keys, (keys.Length - 1, keys.Length), cancellationToken);

        await fabric.PartitionAsync(survivor, former);
        await cluster.RestartNodeAsync(former, cancellationToken);
        await cluster.StopNodeAsync(survivor);
        var (next, _) = await probe.WaitForNewLeaderAsync(Group, repairedTerm, FailoverSteps.Bound, cancellationToken);
        var reader = await cluster.GetCacheAsync<long>(CacheName, lagging, cancellationToken);

        _ = await Assert.That(repairer).IsEqualTo(survivor);
        _ = await Assert.That(next).IsEqualTo(lagging);
        await AssertReadsAsync(reader, keys, cancellationToken);
    }

    /// <summary>
    /// Writes the first keys, stops the lagging follower once it holds them, writes the later keys without it, and waits until the survivor has
    /// compacted its group log past everything the stopped follower holds.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="probe">The leader probe of the cluster.</param>
    /// <param name="members">The leader, the follower that lags and the follower that compacts.</param>
    /// <param name="writes">The cache of the survivor, and the keys of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the survivor compacted its log.</returns>
    private static async Task LagBehindAsync(
        HostedCluster cluster,
        ClusterLeaderProbe<ClusterStartOptions> probe,
        (string Former, string Lagging, string Survivor) members,
        (ICache<long> Cache, string[] Keys) writes,
        CancellationToken cancellationToken)
    {
        await WriteAsync(writes.Cache, writes.Keys, (0, FirstKeys), cancellationToken);
        await probe.Ledger(Group).UntilValueAsync(
            (cluster.Cluster, members.Former, members.Lagging),
            static async (s, token) => (await ReplicaLogReads.ReadAsync(s.Cluster, s.Lagging, Group, token)).LastLogIndex >= (await ReplicaLogReads.ReadAsync(s.Cluster, s.Former, Group, token)).LastLogIndex,
            "the lagging follower holds the first keys",
            cancellationToken);
        var held = (await ReplicaLogReads.ReadAsync(cluster.Cluster, members.Lagging, Group, cancellationToken)).LastLogIndex;
        await cluster.StopNodeAsync(members.Lagging);
        await WriteAsync(writes.Cache, writes.Keys, (FirstKeys, FirstKeys + LaterKeys), cancellationToken);
        await probe.Ledger(Group).UntilValueAsync(
            (cluster.Cluster, members.Survivor, held),
            static async (s, token) => (await ReplicaLogReads.ReadAsync(s.Cluster, s.Survivor, Group, token)).SnapshotIndex > s.held,
            "the survivor compacted its log past the lagging follower",
            cancellationToken);
    }

    /// <summary>Writes keys, each with the value that names its position, retrying while the group cannot take the write.</summary>
    /// <param name="cache">The cache to write through.</param>
    /// <param name="keys">The keys of the group.</param>
    /// <param name="range">The positions of the first key and after the last key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every write was acknowledged.</returns>
    private static async Task WriteAsync(ICache<long> cache, string[] keys, (int From, int To) range, CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();
        for (var i = range.From; i < range.To; i++)
        {
            _ = await Eventually.SucceedsAsync(
                (Cache: cache, Key: keys[i], Value: i + 1L),
                static async (s, token) =>
                {
                    await s.Cache.SetAsync(s.Key, s.Value, cancellationToken: token);
                    return true;
                },
                FailoverSteps.Bound,
                attempts,
                cancellationToken);
        }
    }

    /// <summary>Reads every key and checks the value that names its position.</summary>
    /// <param name="cache">The cache to read through.</param>
    /// <param name="keys">The keys of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every key was checked.</returns>
    private static async Task AssertReadsAsync(ICache<long> cache, string[] keys, CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();
        for (var i = 0; i < keys.Length; i++)
        {
            var read = await Eventually.SucceedsAsync((Cache: cache, Key: keys[i]), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
            _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, i + 1L)).Because($"key {keys[i]}");
        }
    }
}
