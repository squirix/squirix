using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>SDK requests started immediately after a leader loss complete through the surviving entry node without application retries.</summary>
public sealed class LeaderRecoveryRequestTests : EndToEndTestBase
{
    private const string CacheName = "leader-recovery-requests";
    private const string Group = "nodeA";
    private const int Writes = 32;

    /// <summary>A burst during the election keeps every request successful and commits each mutation only once.</summary>
    /// <param name="productTiming">Whether elections use product timing rather than the slower CI tier.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RequestsSurviveLeaderLoss(bool productTiming, CancellationToken cancellationToken)
    {
        var timing = productTiming ? FailoverTiming.ProductDefaults(nameof(RequestsSurviveLeaderLoss)) : FailoverTiming.For(nameof(RequestsSurviveLeaderLoss));
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RequestsSurviveLeaderLoss), FailoverSteps.Options(nameof(RequestsSurviveLeaderLoss), timing: timing), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var cache = await cluster.GetCacheAsync<long>(CacheName, survivors[0], cancellationToken);
        var keys = FailoverSteps.KeysOf(CacheName, Group, "recovery", Writes);
        await cache.SetAsync(keys[0], 0L, cancellationToken: cancellationToken);

        await cluster.AbruptShutdownNodeAsync(former);
        await cluster.StopNodeAsync(former);
        var requests = new Task[Writes];
        for (var i = 0; i < requests.Length; i++)
            requests[i] = cache.SetAsync(keys[i], i + 1L, cancellationToken: cancellationToken);

        // There is no application retry or Eventually wrapper around these operations: any failed SDK call fails the test.
        await Task.WhenAll(requests);
        var (leader, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
        for (var i = 0; i < keys.Length; i++)
            _ = await Assert.That(await cache.GetValueAsync(keys[i], cancellationToken)).IsEqualTo(new CacheValueResult<long>(true, i + 1L));

        var audit = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);
        _ = await Assert.That(audit.ClientEntries).IsEqualTo(Writes + 1);
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
        _ = await Assert.That(leader).IsNotEqualTo(former);
    }
}
