using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A former leader that restarts after its group elected a new leader rejoins as a follower: it adopts the later term, holds no authority,
/// starts no election, and retains the same committed log as the rest of the group, the writes it missed included.
/// </summary>
public sealed class LeaderRestartTests : EndToEndTestBase
{
    private const string CacheName = "leader-restart";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>The leader stops, the majority elects a new leader and commits a write, and the former leader restarts and catches up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task FormerLeaderCatchesUpBeforeEligible(CancellationToken cancellationToken)
    {
        const string testName = nameof(FormerLeaderCatchesUpBeforeEligible);
        var timing = FailoverTiming.For(testName);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var cache = await (await cluster.ConnectClientAsync(survivors[0], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var key = FailoverSteps.KeysOf(CacheName, Group, "rejoin", 1)[0];
        var attempts = new List<EventualAttempt>();
        await cache.SetAsync(key, 1L, cancellationToken: cancellationToken);

        await cluster.StopNodeAsync(former);
        var elected = await probe.WaitForNewLeaderAsync(Group, formerTerm, FailoverSteps.Bound, cancellationToken);
        _ = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
        await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token), FailoverSteps.Bound, attempts, cancellationToken);

        await cluster.RestartNodeAsync(former, cancellationToken);
        var rejoined = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        await probe.AssertNoElectionAsync(Group, timing.Round * 2, cancellationToken);
        var (_, formerAuthority) = probe.Ledger(Group).Observe([former]);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var formerCache = await (await cluster.ConnectClientAsync(former, cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var read = await Eventually.SucceedsAsync((Cache: formerCache, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);

        _ = await Assert.That(elected.Term).IsGreaterThan(formerTerm);
        _ = await Assert.That(rejoined).IsEqualTo(elected);
        _ = await Assert.That(formerAuthority).IsEqualTo(0UL);
        _ = await Assert.That(report.ClientEntries).IsGreaterThanOrEqualTo(2);
        _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, 2L)).Because(Eventually.Dump(attempts));
    }
}
