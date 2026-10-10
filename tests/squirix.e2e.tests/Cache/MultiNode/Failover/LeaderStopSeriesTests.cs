using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
/// The leader of an RF=3 group stops thirty times in a row on a cluster with the product default election timing: after each stop the
/// survivors serve a write and a read through the SDK, the stopped node rejoins and catches up, and the time from the stop to the first success
/// is recorded as timing evidence. No acknowledged write is lost and no term has two leaders.
/// </summary>
[Property(StressSuite.TraitName, StressSuite.TraitValue)]
public sealed class LeaderStopSeriesTests : EndToEndTestBase
{
    private const string CacheName = "leader-stop-series";

    /// <summary>The group under test, named after its owner; with three replicas every node is a member.</summary>
    private const string Group = "nodeA";

    /// <summary>The number of leader stops in the series.</summary>
    private const int Stops = 30;

    /// <summary>The name of the evidence file the series writes.</summary>
    private const string EvidenceFile = "failover-leader-stop-series.json";

    /// <summary>
    /// Stops the current leader thirty times, restarting it and auditing the group log after each stop. The timing is written as evidence;
    /// the 95th percentile is held to its limit only on the controlled machine, elsewhere each stop is held to the recovery bound.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(1_200_000)]
    [NotInParallel]
    public async Task LeaderStopSeriesRecordsTimeline(CancellationToken cancellationToken)
    {
        const string testName = nameof(LeaderStopSeriesRecordsTimeline);

        // The product default timing, not the pull request tier, with a jitter seed fixed per test; the testkit mixes in each node.
        var timing = FailoverTiming.ProductDefaults(testName);
        var options = new MultiNodeStartOptions { ReplicaCount = 3, Failover = true, ElectionTiming = timing };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, options, true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        _ = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);

        var samples = new List<FailoverSample>(Stops);
        var series = new List<(RegisterHistory History, string[] Keys)>(Stops);
        var completed = false;
        try
        {
            for (var i = 1; i <= Stops; i++)
            {
                var (sample, history, keys) = await StopOnceAsync(cluster, probe, i, cancellationToken);
                samples.Add(sample);
                series.Add((history, keys));
                Write(FailoverEvidence.Describe(sample));
            }

            await AssertNothingLostAsync(cluster, series, cancellationToken);
            completed = true;
        }
        finally
        {
            var evidence = FailoverEvidence.Build(testName, timing, [.. samples], completed);
            var path = await FailoverEvidence.WriteAsync(evidence, EvidenceFile, CancellationToken.None);
            Write(string.Create(CultureInfo.InvariantCulture, $"Timing evidence of {samples.Count} stops, p50 {evidence.P50Ms:F0} ms, p95 {evidence.P95Ms:F0} ms ({evidence.Gate}): {path}"));
        }

        var (machine, enforced, reason) = FailoverEvidence.Gate();
        if (enforced)
        {
            var p95 = FailoverEvidence.Build(testName, timing, [.. samples], true).P95Ms;
            _ = await Assert.That(FailoverEvidence.WithinLimit(machine, p95)).IsTrue().Because($"p95 {p95:F0} ms must stay within {FailoverEvidence.P95Limit.TotalMilliseconds:F0} ms ({reason})");
        }
    }

    /// <summary>Stops the leader under a register workload, checks the recovery and the safety invariants, then restarts the node and waits until it caught up.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="probe">The leader probe whose ledger checks election safety throughout.</param>
    /// <param name="iteration">The position in the series, from one.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sample, and the history and the keys of the workload.</returns>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    private static async Task<(FailoverSample Sample, RegisterHistory History, string[] Keys)> StopOnceAsync(
        HostedCluster cluster,
        ClusterLeaderProbe<ClusterStartOptions> probe,
        int iteration,
        CancellationToken cancellationToken)
    {
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            try
            {
                var stopStarted = 0L;
                ValueTask StopLeaderAsync()
                {
                    stopStarted = Stopwatch.GetTimestamp();
                    return cluster.StopNodeAsync(former);
                }

                var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.ThreeNode);
                var fault = (former, $"stop {iteration}: leader {former} stops", (Func<ValueTask>?)StopLeaderAsync);
                var run = await FailoverFault.RunAsync(scene, survivors, $"stop{iteration}", fault, cancellationToken);
                _ = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
                await probe.Ledger(Group).UntilAsync(() => timeline.TimestampOf(FailoverPhase.Converged) != null, "the timeline sees the survivors converge", cancellationToken);
                await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);

                var dump = timeline.Dump() + Eventually.Dump(run.Attempts);
                _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump);
                _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
                _ = await Assert.That(run.StoppedWhileAcked).IsTrue().Because(dump);
                _ = await Assert.That(run.Acked.Term).IsGreaterThan(formerTerm).Because(dump);
                _ = await Assert.That(survivors).Contains(run.Acked.NodeId).Because(dump);
                _ = await Assert.That(run.Workload.History.Check()).IsEmpty().Because(run.Workload.History.Summary() + dump);

                var sample = new FailoverSample(
                    iteration,
                    former,
                    run.Acked.NodeId,
                    run.Acked.Term,
                    run.Elapsed.TotalMilliseconds,
                    Stopwatch.GetElapsedTime(stopStarted, run.Down).TotalMilliseconds,
                    PhaseMs(timeline, FailoverPhase.LeaderLost, stopStarted),
                    PhaseMs(timeline, FailoverPhase.TermRaised, stopStarted),
                    PhaseMs(timeline, FailoverPhase.NewLeader, stopStarted),
                    PhaseMs(timeline, FailoverPhase.Converged, stopStarted));

                await cluster.RestartNodeAsync(former, cancellationToken);
                _ = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
                _ = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
                return (sample, run.Workload.History, run.Keys);
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw new InvalidOperationException($"Leader stop {iteration} failed: {exception.Message}{Environment.NewLine}{timeline.Dump()}", exception);
            }
        }
    }

    /// <summary>Reads every register of every stop once more through a node that ran through the whole series, and checks each history again.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="series">The history and the keys of each stop's workload.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every history passed.</returns>
    private static async Task AssertNothingLostAsync(HostedCluster cluster, List<(RegisterHistory History, string[] Keys)> series, CancellationToken cancellationToken)
    {
        var reader = await cluster.GetCacheAsync<long>(CacheName, FailoverSteps.ThreeNodes[0], cancellationToken);
        for (var i = 0; i < series.Count; i++)
        {
            var (history, keys) = series[i];
            await FailoverSteps.ReadFinalAsync(history, reader, keys, cancellationToken);
            _ = await Assert.That(history.Check()).IsEmpty().Because($"stop {i + 1}: {history.Summary()}");
        }
    }

    private static double? PhaseMs(FailoverTimeline<ClusterStartOptions> timeline, FailoverPhase phase, long stopStarted) =>
        timeline.TimestampOf(phase) is { } at ? Stopwatch.GetElapsedTime(stopStarted, at).TotalMilliseconds : null;

    private static void Write(string line) => TestContext.Current?.Output.WriteLine(line);
}
