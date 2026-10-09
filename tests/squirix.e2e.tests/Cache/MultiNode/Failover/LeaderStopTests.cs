using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
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

    /// <summary>The value of a register the readers must see before the fault starts, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    private const int Reads = 50;
    private const int Writes = 50;

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

    /// <summary>Stops the leader of the group under a register workload, then checks the recovery bound, the history, the ledger and the log audit.</summary>
    /// <param name="testName">The test name, which names the data directory and seeds the election jitter.</param>
    /// <param name="abrupt">Whether the leader shuts down without a graceful drain.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task LeaderStopRecoversAsync(string testName, bool abrupt, CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var timeline = FailoverTimeline<ClusterStartOptions>.Start(cluster.Cluster, Group);
        await using (timeline)
        {
            Func<ValueTask> fault = abrupt ? () => ShutDownAbruptlyAsync(cluster, former) : () => cluster.StopNodeAsync(former);
            var what = abrupt ? $"leader {former} shuts down abruptly" : $"leader {former} stops";
            var run = await RunFaultAsync((Cluster: cluster, Probe: probe, Timeline: timeline), survivors, "registers", what, fault, cancellationToken);
            var (leader, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
            await FailoverSteps.ReadFinalAsync(run.Workload.History, run.Reader, run.Keys, cancellationToken);
            var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

            var dump = timeline.Dump();
            var history = run.Workload.History;
            _ = await Assert.That(run.Read).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
            _ = await Assert.That(run.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump + Eventually.Dump(run.Attempts));
            _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
            _ = await Assert.That(survivors).Contains(leader).Because(dump);
            _ = await Assert.That(term).IsGreaterThan(formerTerm).Because(dump);
            _ = await Assert.That(report.ClientEntries).IsGreaterThan(0);
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

    /// <summary>
    /// Starts a register workload through two clients on the given nodes, starts the fault once the workload made progress, and probes a
    /// write and a read through the first node until both succeed; returns once the fault and the workload ended.
    /// </summary>
    /// <param name="scene">The cluster, the leader probe whose ledger checks election safety throughout, and the timeline the fault is marked on.</param>
    /// <param name="clientNodes">Two nodes that run through the fault: the writer and the probe connect to the first, the reader to the second.</param>
    /// <param name="prefix">The prefix of the register and probe keys, distinct per fault.</param>
    /// <param name="what">The fault, for the timeline.</param>
    /// <param name="fault">Starts the fault and completes once it ended.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the fault recorded.</returns>
    private static async Task<FaultRun> RunFaultAsync(
        (HostedCluster Cluster, ClusterLeaderProbe<ClusterStartOptions> Probe, FailoverTimeline<ClusterStartOptions> Timeline) scene,
        string[] clientNodes,
        string prefix,
        string what,
        Func<ValueTask> fault,
        CancellationToken cancellationToken)
    {
        var writer = await (await scene.Cluster.ConnectClientAsync(clientNodes[0], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var reader = await (await scene.Cluster.ConnectClientAsync(clientNodes[1], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var keys = FailoverSteps.KeysOf(CacheName, Group, prefix, 4);
        var probeKey = FailoverSteps.KeysOf(CacheName, Group, prefix + "-probe", 1)[0];
        var workload = new RegisterWorkload(writer, reader, keys);
        var ledger = scene.Probe.Ledger(Group);

        var running = workload.RunAsync(Writes, Reads, cancellationToken);
        var safety = ledger.UntilAsync(() => running.IsCompleted, "the workload ends", cancellationToken);
        await ledger.UntilValueAsync(
            (Reader: reader, Key: keys[0]),
            static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
            "the workload makes progress",
            cancellationToken);

        var started = Stopwatch.GetTimestamp();
        scene.Timeline.Mark(what);
        var stopping = fault().AsTask();
        var attempts = new List<EventualAttempt>();
        var (read, elapsed) = await FailoverSteps.RecoverAsync(writer, probeKey, 1L, started, attempts, cancellationToken);
        scene.Timeline.Mark($"a write and a read through {clientNodes[0]} succeed");
        await stopping;
        await running;
        await safety;
        return new FaultRun(workload, keys, reader, read, elapsed, attempts);
    }

    /// <summary>What one fault under a register workload recorded.</summary>
    /// <param name="Workload">The workload, with its history.</param>
    /// <param name="Keys">The register keys.</param>
    /// <param name="Reader">The cache the readers read through.</param>
    /// <param name="Read">The value the recovery probe read back.</param>
    /// <param name="Elapsed">The time from the start of the fault until the probe read its value back.</param>
    /// <param name="Attempts">The attempts of the probe.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct FaultRun(RegisterWorkload Workload, string[] Keys, ICache<long> Reader, CacheValueResult<long> Read, TimeSpan Elapsed, List<EventualAttempt> Attempts);
}
