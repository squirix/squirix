using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// When the leader of an RF=3 group stops under a register workload, the surviving majority elects a new leader and serves a write and a
/// read through the SDK within the recovery bound, no acknowledged write is lost, reads stay linearizable, no term has two leaders, and the
/// survivors keep one committed log with every client operation in it once.
/// </summary>
public sealed class LeaderStopTests : EndToEndTestBase
{
    private const string CacheName = "leader-stop";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>A graceful stop of the leader, found at run time, under a register workload.</summary>
    /// <remarks>The recovery bound is measured from the start of the stop, so the time the stop takes counts against it.</remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task MajorityRecoversAfterLeaderStop(CancellationToken cancellationToken) => LeaderStopRecoversAsync(nameof(MajorityRecoversAfterLeaderStop), false, cancellationToken);

    /// <summary>An abrupt shutdown of the leader, found at run time, without a graceful drain, under a register workload.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task RfThreeLeaderStopRecovers(CancellationToken cancellationToken) => LeaderStopRecoversAsync(nameof(RfThreeLeaderStopRecovers), true, cancellationToken);

    /// <summary>
    /// After the leader stops and the majority recovers, the former leader rejoins; then a follower stops, so the new leader and the rejoined
    /// node form the majority. Writes and reads continue through both faults, the second without a leader change, and a write commits while
    /// the follower is down.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task ReadAndWriteContinueOnRfThreeMajority(CancellationToken cancellationToken)
    {
        const string testName = nameof(ReadAndWriteContinueOnRfThreeMajority);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.ThreeNode);
            try
            {
                var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
                var first = await FailoverFault.RunAsync(scene, survivors, "first", (former, $"leader {former} stops", () => cluster.StopNodeAsync(former)), cancellationToken);
                _ = await Assert.That(first.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(first.Attempts));
                _ = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
                var elected = timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue;

                await cluster.RestartNodeAsync(former, cancellationToken);
                var leader = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);

                // Stop the member that is neither the leader nor the rejoined node, so the majority needs the rejoined node; when the rejoined
                // node leads again, stop either other member: the remaining majority still holds the rejoined node, now as its leader.
                var follower = Array.Find(FailoverSteps.ThreeNodes, id => !string.Equals(id, leader.NodeId, StringComparison.Ordinal) && !string.Equals(id, former, StringComparison.Ordinal))
                               ?? FailoverSteps.Except(FailoverSteps.ThreeNodes, leader.NodeId)[0];
                var majority = FailoverSteps.Except(FailoverSteps.ThreeNodes, follower);
                var second = await FailoverFault.RunAsync(scene, majority, "second", (follower, $"follower {follower} stops", () => cluster.StopNodeAsync(follower)), cancellationToken);
                _ = await Assert.That(second.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(second.Attempts));

                await cluster.RestartNodeAsync(follower, cancellationToken);
                _ = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
                await FailoverSteps.ReadFinalAsync(first.Workload.History, second.Reader, first.Keys, cancellationToken);
                await FailoverSteps.ReadFinalAsync(second.Workload.History, second.Reader, second.Keys, cancellationToken);
                _ = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);

                var dump = timeline.Dump();
                _ = await Assert.That(first.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(first.Acked.Term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(second.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(second.Acked).IsEqualTo(leader).Because(dump);
                _ = await Assert.That(second.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(first.Workload.History.Check()).IsEmpty().Because(first.Workload.History.Summary());
                _ = await Assert.That(second.Workload.History.Check()).IsEmpty().Because(second.Workload.History.Summary());
                _ = await Assert.That(FailoverFault.CoversAfter(first.Workload.History, elected)).IsTrue().Because(first.Workload.History.Summary() + dump);
                _ = await Assert.That(FailoverFault.CoversAfter(second.Workload.History, second.Down)).IsTrue().Because(second.Workload.History.Summary() + dump);
                _ = await Assert.That(leader.Term).IsGreaterThan(formerTerm).Because(dump);
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw Dumped(timeline, exception);
            }
        }
    }

    /// <summary>Adds the failover timeline to a failure that is not an assertion, so a slow or stuck phase shows where it stood.</summary>
    /// <param name="timeline">The timeline.</param>
    /// <param name="failure">The failure.</param>
    /// <returns>The failure to throw.</returns>
    private static InvalidOperationException Dumped(FailoverTimeline<ClusterStartOptions> timeline, Exception failure) =>
        new($"The failover scenario failed: {failure.Message}{Environment.NewLine}{timeline.Dump()}", failure);

    /// <summary>Stops the leader of the group under a register workload, then checks the recovery bound, the history, the ledger and the log audit.</summary>
    /// <param name="testName">The test name, which names the data directory and seeds the election jitter.</param>
    /// <param name="abrupt">Whether the leader shuts down without a graceful drain.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    private static async Task LeaderStopRecoversAsync(string testName, bool abrupt, CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            try
            {
                Func<ValueTask> stop = abrupt ? () => ShutDownAbruptlyAsync(cluster, former) : () => cluster.StopNodeAsync(former);
                var what = abrupt ? $"leader {former} shuts down abruptly" : $"leader {former} stops";
                var run = await FailoverFault.RunAsync(new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.ThreeNode), survivors, "registers", (former, what, stop), cancellationToken);
                _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(run.Attempts));
                var (leader, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
                await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);
                var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

                var dump = timeline.Dump();
                var history = run.Workload.History;
                _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(run.Acked.Term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(survivors).Contains(run.Acked.NodeId).Because(dump);
                _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
                _ = await Assert.That(FailoverFault.CoversAfter(history, timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue)).IsTrue().Because(history.Summary() + dump);
                _ = await Assert.That(survivors).Contains(leader).Because(dump);
                _ = await Assert.That(term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(report.ClientEntries).IsGreaterThan(0);
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw Dumped(timeline, exception);
            }
        }
    }

    /// <summary>Shuts a node down without a graceful drain, then removes it from the cluster, so the probes no longer read it.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node.</param>
    /// <returns>A task that completes once the node is removed.</returns>
    private static async ValueTask ShutDownAbruptlyAsync(HostedCluster cluster, string nodeId)
    {
        await cluster.AbruptShutdownNodeAsync(nodeId);
        await cluster.StopNodeAsync(nodeId);
    }
}
