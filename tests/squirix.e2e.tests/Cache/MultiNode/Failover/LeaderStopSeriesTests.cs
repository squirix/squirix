using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
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

    /// <summary>The number of times a stop reads the leader again when another election changed it before the stop began.</summary>
    private const int MaxAttempts = 3;

    /// <summary>The name of the evidence file the series writes.</summary>
    private const string EvidenceFile = "failover-leader-stop-series.json";

    /// <summary>
    /// Stops the current leader thirty times, restarting it and auditing the group log after each stop. The timing is written as evidence,
    /// measured both from the start of the stop and from the moment the stopped node was down; the 95th percentile of the second measure is
    /// held to its limit only on the controlled machine, elsewhere each stop is held to the recovery bound.
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
        FailoverTimingEvidence? evidence = null;
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
            evidence = FailoverEvidence.Build(testName, timing, [.. samples], completed);
            await WriteEvidenceAsync(evidence);
        }

        var (machine, enforced, reason) = FailoverEvidence.Gate();
        if (enforced)
        {
            _ = await Assert.That(FailoverEvidence.WithinLimit(machine, evidence.SinceDownP95Ms)).IsTrue()
                            .Because($"p95 of the recovery since the node was down, {evidence.SinceDownP95Ms:F0} ms, must stay within {FailoverEvidence.P95Limit.TotalMilliseconds:F0} ms ({reason})");
        }
    }

    /// <summary>Writes the evidence file and reports where; a write that fails is reported to the test output and does not replace the failure of the test.</summary>
    /// <param name="evidence">The evidence.</param>
    /// <returns>A task that completes once the file is written or the failure is reported.</returns>
    private static async Task WriteEvidenceAsync(FailoverTimingEvidence evidence)
    {
        try
        {
            var path = await FailoverEvidence.WriteAsync(evidence, EvidenceFile, CancellationToken.None);
            Write(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Timing evidence of {evidence.Samples.Length} stops: from stop start p50 {evidence.FromStopStartP50Ms:F0} ms, p95 {evidence.FromStopStartP95Ms:F0} ms; since down p50 {evidence.SinceDownP50Ms:F0} ms, p95 {evidence.SinceDownP95Ms:F0} ms ({evidence.Gate}): {path}"));
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            Write($"The timing evidence could not be written to {FailoverEvidence.EvidenceDirectory()}: {exception.Message}");
        }
    }

    /// <summary>Stops the leader once, reading the leader again when another election changed it before the stop began.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="probe">The leader probe whose ledger checks election safety throughout.</param>
    /// <param name="iteration">The position in the series, from one.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sample, and the history and the keys of the workload.</returns>
    /// <exception cref="InvalidOperationException">The leader changed before the stop in every attempt, or a step other than an assertion failed.</exception>
    private static async Task<(FailoverSample Sample, RegisterHistory History, string[] Keys)> StopOnceAsync(
        HostedCluster cluster,
        ClusterLeaderProbe<ClusterStartOptions> probe,
        int iteration,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (await StopWhenLeaderIsStableAsync(cluster, probe, iteration, cancellationToken) is { } result)
                return result;
        }

        throw new InvalidOperationException($"Leader stop {iteration} found another leader than the one it read in each of {MaxAttempts} attempts.");
    }

    /// <summary>Stops the leader under a register workload, checks the recovery and the safety invariants, then restarts the node and waits until it caught up.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="probe">The leader probe whose ledger checks election safety throughout.</param>
    /// <param name="iteration">The position in the series, from one.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sample, and the history and the keys of the workload; <see langword="null" /> when the leader changed before the stop began and nothing was stopped.</returns>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the failover timeline.</exception>
    private static async Task<(FailoverSample Sample, RegisterHistory History, string[] Keys)?> StopWhenLeaderIsStableAsync(
        HostedCluster cluster,
        ClusterLeaderProbe<ClusterStartOptions> probe,
        int iteration,
        CancellationToken cancellationToken)
    {
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        var clients = cluster.ClientCount;
        await using (timeline)
        {
            try
            {
                var stopStarted = 0L;
                var stale = false;

                ValueTask StopLeaderAsync()
                {
                    stopStarted = Stopwatch.GetTimestamp();
                    stale = timeline.Baseline != (former, formerTerm) || probe.Ledger(Group).Observe(FailoverSteps.ThreeNodes) != (former, formerTerm);
                    return stale ? throw new InvalidOperationException($"The leader of {Group} changed before the stop began.") : cluster.StopNodeAsync(former);
                }

                var scene = new FailoverFault.Scene(cluster, probe, timeline, CacheName, Group, KeyOwnerHelper.ThreeNode);
                var fault = (former, $"stop {iteration}: leader {former} stops", (Func<ValueTask>?)StopLeaderAsync);
                FailoverFault.FaultRun run;
                try
                {
                    run = await FailoverFault.RunAsync(scene, survivors, $"stop{iteration}", fault, cancellationToken);
                }
                catch (InvalidOperationException) when (stale)
                {
                    await cluster.DisposeClientsAfterAsync(clients);
                    return null;
                }

                _ = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
                await probe.Ledger(Group).UntilAsync(() => timeline.TimestampOf(FailoverPhase.Converged) != null, "the timeline sees the survivors converge", cancellationToken);
                await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);

                var history = run.Workload.History;
                var sample = await RecordAsync(timeline, run, (iteration, former, formerTerm, survivors), stopStarted, cluster.Cluster.LastStopOf(former));

                await cluster.DisposeClientsAfterAsync(clients);
                await cluster.RestartNodeAsync(former, cancellationToken);
                _ = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
                _ = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
                return (sample, history, run.Keys);
            }
            catch (Exception exception) when (exception is not AssertionException)
            {
                throw new InvalidOperationException($"Leader stop {iteration} failed: {exception.Message}{Environment.NewLine}{timeline.Dump()}", exception);
            }
        }
    }

    /// <summary>Builds the sample of a stop and asserts the recovery, the workload coverage after the election and the validity of the timeline.</summary>
    /// <param name="timeline">The timeline of the stop.</param>
    /// <param name="run">What the stop recorded.</param>
    /// <param name="stop">The position in the series, the leader that stopped with its term, and the survivors.</param>
    /// <param name="stopStarted">When the stop began, in <see cref="Stopwatch" /> ticks.</param>
    /// <param name="phases">Where the time of the stop went, as the host measured it; <see langword="null" /> when it did not report it.</param>
    /// <returns>The sample.</returns>
    private static async Task<FailoverSample> RecordAsync(
        FailoverTimeline<ClusterStartOptions> timeline,
        FailoverFault.FaultRun run,
        (int Iteration, string Former, ulong FormerTerm, string[] Survivors) stop,
        long stopStarted,
        NodeStopPhases? phases)
    {
        var dump = timeline.Dump() + Eventually.Dump(run.Attempts);
        var history = run.Workload.History;
        var stopDuration = Stopwatch.GetElapsedTime(stopStarted, run.Down).TotalMilliseconds;
        var sample = new FailoverSample(
            stop.Iteration,
            stop.Former,
            run.Acked.NodeId,
            run.Acked.Term,
            run.Elapsed.TotalMilliseconds,
            stopDuration,
            run.Elapsed.TotalMilliseconds - stopDuration,
            PhaseMs(timeline, FailoverPhase.LeaderLost, stopStarted),
            PhaseMs(timeline, FailoverPhase.TermRaised, stopStarted),
            PhaseMs(timeline, FailoverPhase.NewLeader, stopStarted),
            PhaseMs(timeline, FailoverPhase.Converged, stopStarted),
            phases?.HostStopMs,
            phases?.DisposeMs,
            phases?.PersistenceReleaseMs,
            phases?.TotalMs,
            phases?.InFlightAtStop,
            phases?.LastRequestFinishedMs);
        _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump);
        _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
        _ = await Assert.That(run.StoppedWhileAcked).IsTrue().Because(dump);
        _ = await Assert.That(run.Acked.Term).IsGreaterThan(stop.FormerTerm).Because(dump);
        _ = await Assert.That(stop.Survivors).Contains(run.Acked.NodeId).Because(dump);
        _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary() + dump);
        _ = await Assert.That(FailoverFault.CoversAfter(history, timeline.TimestampOf(FailoverPhase.NewLeader) ?? long.MaxValue)).IsTrue().Because(history.Summary() + dump);
        _ = await Assert.That(sample.RecoverySinceDownMs).IsGreaterThanOrEqualTo(0).Because(dump);
        _ = await Assert.That(HasNegativePhase(sample)).IsFalse().Because($"a phase was seen before the stop began: {FailoverEvidence.Describe(sample)}{Environment.NewLine}{dump}");
        return sample;
    }

    /// <summary>Reads every register of every stop once more through the first node, which serves the group again after its last restart, and checks each history again.</summary>
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

    private static bool HasNegativePhase(FailoverSample sample) => sample.LeaderLostMs < 0 || sample.TermRaisedMs < 0 || sample.NewLeaderMs < 0 || sample.ConvergedMs < 0;

    private static double? PhaseMs(FailoverTimeline<ClusterStartOptions> timeline, FailoverPhase phase, long stopStarted) =>
        timeline.TimestampOf(phase) is { } at ? Stopwatch.GetElapsedTime(stopStarted, at).TotalMilliseconds : null;

    private static void Write(string line) => TestContext.Current?.Output.WriteLine(line);
}
