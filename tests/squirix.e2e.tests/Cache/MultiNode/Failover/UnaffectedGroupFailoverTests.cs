using System;
using System.Collections.Generic;
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
/// On five nodes with three replicas, each node leads some of the five groups. When the leader of one group stops, the groups whose leaders
/// keep running are not interrupted: a register workload over their keys sees no failed or unknown call from before the stop until after the
/// affected group recovered, those groups keep their leaders and terms, and the affected group elects a new leader and serves.
/// </summary>
public sealed class UnaffectedGroupFailoverTests : EndToEndTestBase
{
    private const string CacheName = "unaffected-groups";

    /// <summary>The group whose leader stops, named after its owner.</summary>
    private const string Group = "nodeA";

    /// <summary>The value of a register the readers must see before the fault starts, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    /// <summary>The writes, and the reads, each register runs after the affected group recovered, so the workload covers the time after it.</summary>
    private const int TailOperations = 10;

    /// <summary>
    /// The leader of one group stops while a register workload runs over the keys of every group whose leader is another node. Not one of
    /// its calls fails, none ends with an unknown outcome, and many start between the loss of the leader and the election of the new one.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(180_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task UnaffectedGroupsKeepServing(CancellationToken cancellationToken)
    {
        const string testName = nameof(UnaffectedGroupsKeepServing);
        await using var cluster = await HostedCluster.StartFiveNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var leaders = await FindLeadersAsync(probe, cancellationToken);
        var victim = leaders[Group].NodeId;
        var others = new List<string>(leaders.Count);
        foreach (var (group, leader) in leaders)
        {
            if (!string.Equals(leader.NodeId, victim, StringComparison.Ordinal))
                others.Add(group);
        }

        string[] untouched = [.. others];

        var survivors = FailoverSteps.Except(FailoverSteps.FiveNodes, victim);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            try
            {
                var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.FiveNode);
                var (run, unaffected) = await StopLeaderUnderLoadAsync(scene, survivors, untouched, victim, cancellationToken);
                var (leader, term) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.Except(FailoverSteps.MembersOf(FailoverSteps.FiveNodes, Group, 3), victim), FailoverSteps.Bound, cancellationToken);
                await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);
                _ = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.Except(FailoverSteps.MembersOf(FailoverSteps.FiveNodes, Group, 3), victim), FailoverSteps.Bound, cancellationToken);

                var dump = timeline.Dump();
                var history = unaffected.History;
                var lost = timeline.TimestampOf(FailoverPhase.LeaderLost) ?? long.MaxValue;
                var elected = timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue;
                var (writesSinceLoss, readsSinceLoss) = history.StartedAfter(lost);
                var (writesSinceElection, readsSinceElection) = history.StartedAfter(elected);
                _ = await Assert.That(untouched.Length).IsGreaterThanOrEqualTo(2).Because(dump);
                _ = await Assert.That((history.AmbiguousWrites, history.FailedReads)).IsEqualTo((0, 0)).Because(history.Summary() + dump);
                _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
                _ = await Assert.That(writesSinceLoss).IsGreaterThan(writesSinceElection).Because($"no write of an unaffected group ran while the group had no leader; {history.Summary()}{dump}");
                _ = await Assert.That(readsSinceLoss).IsGreaterThan(readsSinceElection).Because($"no read of an unaffected group ran while the group had no leader; {history.Summary()}{dump}");
                _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump + Eventually.Dump(run.Attempts));
                _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(run.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(term).IsGreaterThan(leaders[Group].Term).Because(dump);
                _ = await Assert.That(leader).IsNotEqualTo(victim).Because(dump);
                _ = await Assert.That(run.Workload.History.Check()).IsEmpty().Because(run.Workload.History.Summary());
                foreach (var group in untouched)
                    _ = await Assert.That(probe.Ledger(group).Observe()).IsEqualTo(leaders[group]).Because($"group {group} must keep its leader and term; {dump}");
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw new InvalidOperationException($"The failover scenario failed: {exception.Message}{Environment.NewLine}{timeline.Dump()}", exception);
            }
        }
    }

    /// <summary>Waits until each of the five groups has a stable leader among its three members.</summary>
    /// <param name="probe">The leader probe of the cluster.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader and the term of each group, by group.</returns>
    private static async Task<Dictionary<string, (string NodeId, ulong Term)>> FindLeadersAsync(ClusterLeaderProbe<ClusterStartOptions> probe, CancellationToken cancellationToken)
    {
        var leaders = new Dictionary<string, (string NodeId, ulong Term)>(StringComparer.Ordinal);
        foreach (var group in FailoverSteps.FiveNodes)
            leaders.Add(group, await probe.WaitForStableLeaderAsync(group, FailoverSteps.MembersOf(FailoverSteps.FiveNodes, group, 3), FailoverSteps.Bound, cancellationToken));

        return leaders;
    }

    /// <summary>
    /// Starts a register workload over the keys of the given groups, waits until it made progress, stops the node under it with the affected
    /// group's own workload, and ends the first workload once the affected group recovered and the second workload ran its tail.
    /// </summary>
    /// <param name="scene">The cluster and the affected group.</param>
    /// <param name="survivors">The nodes the clients connect to, which run through the fault.</param>
    /// <param name="untouched">The groups whose leaders keep running.</param>
    /// <param name="victim">The node that stops.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the fault of the affected group recorded, and the workload over the unaffected groups.</returns>
    private static async Task<(FailoverFault.FaultRun Run, RegisterWorkload Unaffected)> StopLeaderUnderLoadAsync(
        FailoverFault.Scene scene,
        string[] survivors,
        string[] untouched,
        string victim,
        CancellationToken cancellationToken)
    {
        var writer = await scene.Cluster.GetCacheAsync<long>(CacheName, survivors[0], cancellationToken);
        var reader = await scene.Cluster.GetCacheAsync<long>(CacheName, survivors[1], cancellationToken);
        var keys = new string[untouched.Length * 2];
        for (var i = 0; i < untouched.Length; i++)
        {
            keys[2 * i] = FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, untouched[i], "unaffected-" + untouched[i], 1)[0];
            keys[(2 * i) + 1] = FailoverSteps.KeysOf(KeyOwnerHelper.FiveNode, CacheName, untouched[i], "unaffected-b-" + untouched[i], 1)[0];
        }

        var unaffected = new RegisterWorkload(writer, reader, keys);
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = unaffected.RunUntilAsync(recovered.Task, TailOperations, cancellationToken);
        try
        {
            await scene.Probe.Ledger(Group).UntilValueAsync(
                (Reader: reader, Key: keys[0]),
                static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
                "the workload over the unaffected groups makes progress",
                cancellationToken);

            var run = await FailoverFault.RunAsync(scene, survivors, "affected", (victim, $"leader {victim} stops", null), cancellationToken);
            recovered.SetResult();
            await running;
            return (run, unaffected);
        }
        catch
        {
            // Ends the workload, and waits for it, so it does not outlive the cluster; the first failure is the one rethrown.
            _ = recovered.TrySetResult();
            await running.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
    }
}
