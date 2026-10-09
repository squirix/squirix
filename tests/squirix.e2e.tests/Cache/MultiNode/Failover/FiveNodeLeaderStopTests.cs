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
/// An RF=5 group on five nodes survives two leader stops in a row: after each one the remaining majority elects a leader in a later term and
/// serves a write and a read through the SDK, no acknowledged write is lost, reads stay linearizable, and the three last members keep one
/// committed log with every client operation in it once.
/// </summary>
public sealed class FiveNodeLeaderStopTests : EndToEndTestBase
{
    private const string CacheName = "five-node-leader-stop";

    /// <summary>The group under test, named after its owner; with five replicas every node is a member.</summary>
    private const string Group = "nodeA";

    /// <summary>
    /// The leader of the group stops under a register workload and a new leader is elected among the four survivors; then that leader stops
    /// under a second workload, and the three nodes left, still a majority of five, elect a third leader and serve.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(180_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RfFiveSurvivesTwoLeaderStops(CancellationToken cancellationToken)
    {
        const string testName = nameof(RfFiveSurvivesTwoLeaderStops);
        await using var cluster = await HostedCluster.StartFiveNodeAsync(testName, FailoverSteps.Options(testName, replicaCount: 5), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (first, firstTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.FiveNodes, FailoverSteps.Bound, cancellationToken);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.FiveNode);
            try
            {
                var fourSurvivors = FailoverSteps.Except(FailoverSteps.FiveNodes, first);
                var one = await FailoverFault.RunAsync(scene, fourSurvivors, "first", (first, $"leader {first} stops", null), cancellationToken);
                _ = await Assert.That(one.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(one.Attempts));
                var (second, secondTerm) = await probe.WaitForStableLeaderAsync(Group, fourSurvivors, FailoverSteps.Bound, cancellationToken);
                var elected = timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue;

                var threeSurvivors = FailoverSteps.Except(fourSurvivors, second);
                var two = await FailoverFault.RunAsync(scene, threeSurvivors, "second", (second, $"leader {second} stops", null), cancellationToken);
                _ = await Assert.That(two.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(two.Attempts));
                var (third, thirdTerm) = await probe.WaitForStableLeaderAsync(Group, threeSurvivors, FailoverSteps.Bound, cancellationToken);
                var thirdElected = timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue;

                await FailoverSteps.ReadFinalAsync(one.Workload.History, two.Reader, one.Keys, cancellationToken);
                await FailoverSteps.ReadFinalAsync(two.Workload.History, two.Reader, two.Keys, cancellationToken);
                var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, threeSurvivors, FailoverSteps.Bound, cancellationToken);

                var dump = timeline.Dump();
                _ = await Assert.That(one.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(two.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(one.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(two.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(fourSurvivors).Contains(second).Because(dump);
                _ = await Assert.That(threeSurvivors).Contains(third).Because(dump);
                _ = await Assert.That(secondTerm).IsGreaterThan(firstTerm).Because(dump);
                _ = await Assert.That(thirdTerm).IsGreaterThan(secondTerm).Because(dump);
                _ = await Assert.That(one.Acked.Term).IsGreaterThan(firstTerm).Because(dump);
                _ = await Assert.That(two.Acked.Term).IsGreaterThan(secondTerm).Because(dump);
                _ = await Assert.That(one.Workload.History.Check()).IsEmpty().Because(one.Workload.History.Summary());
                _ = await Assert.That(two.Workload.History.Check()).IsEmpty().Because(two.Workload.History.Summary());
                _ = await Assert.That(FailoverFault.CoversAfter(one.Workload.History, elected)).IsTrue().Because(one.Workload.History.Summary() + dump);
                _ = await Assert.That(FailoverFault.CoversAfter(two.Workload.History, thirdElected)).IsTrue().Because(two.Workload.History.Summary() + dump);
                _ = await Assert.That(report.ClientEntries).IsGreaterThan(0);
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw new InvalidOperationException($"The failover scenario failed: {exception.Message}{Environment.NewLine}{timeline.Dump()}", exception);
            }
        }
    }
}
