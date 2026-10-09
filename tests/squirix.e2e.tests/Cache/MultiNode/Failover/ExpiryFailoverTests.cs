using System;
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
/// An entry a client saw expire stays expired after its group fails over to a leader whose wall clock runs behind: the new leader does not
/// bring it back, though by its own clock the entry is still live.
/// </summary>
public sealed class ExpiryFailoverTests : EndToEndTestBase
{
    private const string CacheName = "expiry-failover";

    /// <summary>The only node on the system wall clock; the other two run an hour behind.</summary>
    private const string SystemClockNode = "nodeA";

    private static readonly SkewedTimeProvider Behind = new(TimeSpan.FromHours(-1));

    private static readonly TimeSpan Lifetime = TimeSpan.FromSeconds(2);

    /// <summary>
    /// A group led by the system-clock node writes an entry with a two-second lifetime; once a read through the SDK finds it expired, the
    /// leader stops, and reads through both survivors, one of which now leads an hour behind, still find it absent. By the clock of the new
    /// leader the entry has not yet expired when it is read.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task ExpiredEntryDoesNotReappearAfterFailover(CancellationToken cancellationToken)
    {
        const string testName = nameof(ExpiredEntryDoesNotReappearAfterFailover);
        var options = FailoverSteps.Options(testName, static nodeId => string.Equals(nodeId, SystemClockNode, StringComparison.Ordinal) ? null : Behind);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, options, true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (group, formerTerm) = await FindGroupLedByAsync(probe, SystemClockNode, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, SystemClockNode);
        var first = await (await cluster.ConnectClientAsync(survivors[0], cancellationToken)).GetCacheAsync<string>(CacheName, cancellationToken);
        var second = await (await cluster.ConnectClientAsync(survivors[1], cancellationToken)).GetCacheAsync<string>(CacheName, cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, group, "expiry");
        var written = TimeProvider.System.GetUtcNow();
        await first.SetAsync(key, "ephemeral", Expiry.In(Lifetime), cancellationToken);

        // Waits for real time to pass the deadline: the read on the leader decides the expiry and commits it.
        await probe.Ledger(group).UntilValueAsync(
            (Cache: first, Key: key),
            static async (s, token) => !(await s.Cache.GetValueAsync(s.Key, token)).Found,
            "a read finds the entry expired",
            cancellationToken);

        await cluster.StopNodeAsync(SystemClockNode);
        var (leader, term) = await probe.WaitForNewLeaderAsync(group, formerTerm, FailoverSteps.Bound, cancellationToken);
        _ = await probe.WaitForStableLeaderAsync(group, survivors, FailoverSteps.Bound, cancellationToken);
        var attempts = new List<EventualAttempt>();
        var throughFirst = await Eventually.SucceedsAsync((Cache: first, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var throughSecond = await Eventually.SucceedsAsync((Cache: second, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var leaderClock = cluster.Cluster[leader].GetRequiredService<TimeProvider>();
        var leaderNow = leaderClock.GetUtcNow();

        _ = await Assert.That(leaderClock).IsSameReferenceAs(Behind);
        _ = await Assert.That(written + Lifetime).IsGreaterThan(leaderNow).Because("By the clock of the new leader the entry must still be live, or the reads prove nothing.");
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
        _ = await Assert.That(throughFirst.Found).IsFalse().Because(Eventually.Dump(attempts));
        _ = await Assert.That(throughSecond.Found).IsFalse().Because(Eventually.Dump(attempts));
    }

    /// <summary>Finds a group a given node leads stably; with three replicas every node is a member of every group.</summary>
    /// <param name="probe">The leader probe.</param>
    /// <param name="nodeId">The node that must lead.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The group, named after its owner, and the term the node leads it in.</returns>
    /// <exception cref="InvalidOperationException">The node leads no group.</exception>
    private static async Task<(string Group, ulong Term)> FindGroupLedByAsync(ClusterLeaderProbe<ClusterStartOptions> probe, string nodeId, CancellationToken cancellationToken)
    {
        foreach (var group in FailoverSteps.ThreeNodes)
        {
            var (leader, term) = await probe.WaitForStableLeaderAsync(group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
            if (string.Equals(leader, nodeId, StringComparison.Ordinal))
                return (group, term);
        }

        throw new InvalidOperationException($"Node {nodeId} leads no group, so no group fails over away from it.");
    }
}
