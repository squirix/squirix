using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Stops one node under a register workload and measures how the group recovers; the leader stop tests on five nodes share it.</summary>
internal static class FailoverFault
{
    /// <summary>The value of a register the readers must see before the fault starts, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    /// <summary>The writes, and the reads, each register runs after the recovery probe succeeded, so the workload covers the recovered group.</summary>
    private const int TailOperations = 10;

    /// <summary>Tells whether a history holds an acknowledged write and a successful read that both started at or after a point in time.</summary>
    /// <param name="history">The history.</param>
    /// <param name="timestamp">The point in time, in <see cref="Stopwatch" /> ticks.</param>
    /// <returns><see langword="true" /> when the history checks calls after that point, not only before it.</returns>
    internal static bool CoversAfter(RegisterHistory history, long timestamp)
    {
        var (writes, reads) = history.StartedAfter(timestamp);
        return writes > 0 && reads > 0;
    }

    /// <summary>
    /// Starts a register workload through two clients on the given nodes, stops a node once the workload made progress, and once the node is
    /// down probes a write and a read through the first client node until both succeed. The workload runs until the probe succeeded and then
    /// a few more operations per register; returns once it ended.
    /// </summary>
    /// <param name="scene">The cluster, the group and its keys, the leader probe whose ledger checks election safety throughout, and the timeline.</param>
    /// <param name="clientNodes">Nodes that run through the fault: the writer and the probe connect to the first, the reader to the second.</param>
    /// <param name="prefix">The prefix of the register and probe keys, distinct per fault.</param>
    /// <param name="fault">The node that stops, the fault for the timeline, and the stop, which completes once the node is down; <see langword="null" /> stops the node gracefully.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the fault recorded.</returns>
    internal static async Task<FaultRun> RunAsync(Scene scene, string[] clientNodes, string prefix, (string NodeId, string What, Func<ValueTask>? Stop) fault, CancellationToken cancellationToken)
    {
        var writer = await (await scene.Cluster.ConnectClientAsync(clientNodes[0], cancellationToken)).GetCacheAsync<long>(scene.CacheName, cancellationToken);
        var reader = await (await scene.Cluster.ConnectClientAsync(clientNodes[1], cancellationToken)).GetCacheAsync<long>(scene.CacheName, cancellationToken);
        var keys = FailoverSteps.KeysOf(scene.Ring, scene.CacheName, scene.Group, prefix, 4);
        var probeKey = FailoverSteps.KeysOf(scene.Ring, scene.CacheName, scene.Group, prefix + "-probe", 1)[0];
        var registers = new RegisterWorkload(writer, reader, keys);
        var ledger = scene.Probe.Ledger(scene.Group);
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = registers.RunUntilAsync(recovered.Task, TailOperations, cancellationToken);
        var safety = ledger.UntilAsync(() => running.IsCompleted, "the workload ends", cancellationToken);
        Task? stopping = null;
        try
        {
            await ledger.UntilValueAsync(
                (Reader: reader, Key: keys[0]),
                static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
                "the workload makes progress",
                cancellationToken);

            var started = Stopwatch.GetTimestamp();
            scene.Timeline.Mark(fault.What);
            var stop = fault.Stop ?? (() => scene.Cluster.StopNodeAsync(fault.NodeId));
            stopping = stop().AsTask();
            await stopping;
            var down = Stopwatch.GetTimestamp();
            scene.Timeline.Mark($"{fault.NodeId} is down");

            // The probe starts only once the node is down, so a stopped leader cannot acknowledge it in its own term.
            var attempts = new List<EventualAttempt>();
            var (read, elapsed) = await FailoverSteps.RecoverAsync(writer, probeKey, 1L, started, attempts, cancellationToken);
            var acked = ledger.Observe();
            var stoppedWhileAcked = !scene.Cluster.Cluster.TryGetNode(fault.NodeId, out _);
            scene.Timeline.Mark($"a write and a read through {clientNodes[0]} succeed under {acked.NodeId} in term {acked.Term}");
            recovered.SetResult();
            await running;
            await safety;
            return new FaultRun(registers, keys, reader, read, elapsed, attempts, acked, stoppedWhileAcked, down);
        }
        catch
        {
            // Ends the workload, and waits for every started task, so none outlives the cluster; the first failure is the one rethrown.
            _ = recovered.TrySetResult();
            await Task.WhenAll(stopping ?? Task.CompletedTask, running, safety).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
    }

    /// <summary>The cluster, the group under test and the means to observe it.</summary>
    /// <param name="Cluster">The cluster.</param>
    /// <param name="Probe">The leader probe whose ledger checks election safety throughout.</param>
    /// <param name="Timeline">The timeline the fault is marked on.</param>
    /// <param name="CacheName">The cache the registers live in.</param>
    /// <param name="Group">The group under test, named after its owner.</param>
    /// <param name="Ring">The key ring of the cluster.</param>
    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Scene(
        HostedCluster Cluster,
        ClusterLeaderProbe<ClusterStartOptions> Probe,
        FailoverTimeline<ClusterStartOptions> Timeline,
        string CacheName,
        string Group,
        KeyOwnerHelper Ring);

    /// <summary>What one fault under a register workload recorded.</summary>
    /// <param name="Workload">The workload, with its history.</param>
    /// <param name="Keys">The register keys.</param>
    /// <param name="Reader">The cache the readers read through.</param>
    /// <param name="Read">The value the recovery probe read back.</param>
    /// <param name="Elapsed">The time from the start of the fault until the probe read its value back.</param>
    /// <param name="Attempts">The attempts of the probe.</param>
    /// <param name="Acked">The node holding authority over the group right after the probe succeeded, and its term.</param>
    /// <param name="StoppedWhileAcked">Whether the stopped node was still down right after the probe succeeded.</param>
    /// <param name="Down">When the stopped node was down, in <see cref="Stopwatch" /> ticks.</param>
    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct FaultRun(
        RegisterWorkload Workload,
        string[] Keys,
        ICache<long> Reader,
        CacheValueResult<long> Read,
        TimeSpan Elapsed,
        List<EventualAttempt> Attempts,
        (string NodeId, ulong Term) Acked,
        bool StoppedWhileAcked,
        long Down);
}
