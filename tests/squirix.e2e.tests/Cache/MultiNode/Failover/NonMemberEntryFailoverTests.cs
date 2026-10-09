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
/// On five nodes with three replicas, a group has two nodes outside it. When its leader stops, clients that only reach those two nodes keep
/// writing and reading: each entry node leaves the unreachable leader, learns the new one from the members, and routes to it. No acknowledged
/// write is lost and reads stay linearizable.
/// </summary>
public sealed class NonMemberEntryFailoverTests : EndToEndTestBase
{
    private const string CacheName = "non-member-entry";

    /// <summary>The group under test, named after its owner; its members are this node and the two after it on the ring.</summary>
    private const string Group = "nodeA";

    /// <summary>The members of <see cref="Group" /> with three replicas on five nodes.</summary>
    private static readonly string[] Members = FailoverSteps.MembersOf(FailoverSteps.FiveNodes, Group, 3);

    /// <summary>The nodes outside <see cref="Group" />, which the clients of the test reach alone.</summary>
    private static readonly string[] Entries = FailoverSteps.Except(FailoverSteps.Except(FailoverSteps.Except(FailoverSteps.FiveNodes, Members[0]), Members[1]), Members[2]);

    /// <summary>
    /// The leader of the group stops under a register workload whose writer and reader connect to the two nodes outside the group. The
    /// members elect a new leader, the entry nodes route to it, and both learn it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(180_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task NonMemberEntriesReachNewLeader(CancellationToken cancellationToken)
    {
        const string testName = nameof(NonMemberEntriesReachNewLeader);
        await using var cluster = await HostedCluster.StartFiveNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, Members, FailoverSteps.Bound, cancellationToken);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            try
            {
                var survivors = FailoverSteps.Except(Members, former);
                var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.FiveNode);
                var run = await FailoverFault.RunAsync(scene, Entries, "entries", (former, $"leader {former} stops"), cancellationToken);
                _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(timeline.Dump() + Eventually.Dump(run.Attempts));
                var (leader, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
                await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);
                var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

                var dump = timeline.Dump();
                var history = run.Workload.History;
                _ = await Assert.That(Entries).DoesNotContain(former).Because(dump);
                _ = await Assert.That(probe.Serves(Entries[0], Group)).IsFalse().Because($"{Entries[0]} must be outside the group, or the test proves nothing about non-member entry nodes");
                _ = await Assert.That(probe.Serves(Entries[1], Group)).IsFalse().Because($"{Entries[1]} must be outside the group, or the test proves nothing about non-member entry nodes");
                _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(run.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(survivors).Contains(leader).Because(dump);
                _ = await Assert.That(term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(run.Acked.Term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
                _ = await Assert.That(FailoverFault.CoversAfter(history, timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue)).IsTrue().Because(history.Summary() + dump);
                _ = await Assert.That(report.ClientEntries).IsGreaterThan(0);
                foreach (var entry in Entries)
                {
                    _ = await Assert.That(probe.TryGetLearnedLeader(entry, Group, out var learned)).IsTrue().Because($"{entry} must have learned the new leader; {dump}");
                    _ = await Assert.That(learned.NodeId).IsEqualTo(leader).Because($"{entry} must route to the new leader; {dump}");
                }
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw new InvalidOperationException($"The failover scenario failed: {exception.Message}{Environment.NewLine}{timeline.Dump()}", exception);
            }
        }
    }
}
