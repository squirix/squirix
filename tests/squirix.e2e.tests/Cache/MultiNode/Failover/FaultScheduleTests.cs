using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A seeded pseudo-random schedule of faults runs against a cluster under a register workload: nodes stop gracefully or abruptly, restart,
/// are isolated and heal, while a majority of the replicas stays alive and connected. After every step the group elects a leader, serves a write
/// and a read through the SDK, keeps one leader per term and one committed log; at the end no acknowledged write of any step is lost.
/// </summary>
[Property(StressSuite.TraitName, StressSuite.TraitValue)]
public sealed class FaultScheduleTests : EndToEndTestBase
{
    private const string CacheName = "fault-schedule";

    /// <summary>The group under test, named after its owner; with as many replicas as nodes every node is a member.</summary>
    private const string Group = "nodeA";

    /// <summary>The value of a register the readers must see before a step starts its fault, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    /// <summary>The number of registers of each step.</summary>
    private const int Registers = 3;

    /// <summary>The writes, and the reads, each register runs after the recovery probe of a step succeeded.</summary>
    private const int TailOperations = 10;

    /// <summary>The time between two operations of one writer or reader, which keeps the writes of a whole schedule far below fifty thousand.</summary>
    private static readonly TimeSpan Pace = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Runs the schedule on a cluster with the given number of replicas, one node per replica, and checks the invariants after every step and
    /// at the end. The seed comes from the <c language="csharp">SQUIRIX_FAULT_SEED</c> variable when set, otherwise from the test case name;
    /// it is in the output and in every failure.
    /// </summary>
    /// <param name="replicas">The replica factor, which is also the number of nodes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">A step other than an assertion failed; the message carries the seed and the steps run so far.</exception>
    [Test]
    [Timeout(900_000)]
    [ParallelLimiter<FailoverLimit>]
    [Arguments(3)]
    [Arguments(5)]
    public async Task SeededFaultScheduleKeepsInvariants(int replicas, CancellationToken cancellationToken)
    {
        var testName = $"{nameof(SeededFaultScheduleKeepsInvariants)}-rf{replicas.ToString(CultureInfo.InvariantCulture)}";
        var nodes = replicas == 3 ? FailoverSteps.ThreeNodes : FailoverSteps.FiveNodes;
        var seed = FaultSchedule.SeedFor(testName);
        var trace = new List<string>();
        TestContext.Current?.Output.WriteLine($"{testName}: fault schedule seed {seed.ToString(CultureInfo.InvariantCulture)}");

        await using var fabric = new PartitionFabric();
        var options = new MultiNodeStartOptions { ReplicaCount = replicas, Failover = true, ElectionTiming = FailoverTiming.For(testName), PartitionFabric = fabric };
        await using var cluster = replicas == 3
            ? await HostedCluster.StartThreeNodeAsync(testName, options, true, cancellationToken)
            : await HostedCluster.StartFiveNodeAsync(testName, options, true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        _ = await probe.WaitForStableLeaderAsync(Group, nodes, FailoverSteps.Bound, cancellationToken);

        var anchor = nodes[^1];
        var ring = replicas == 3 ? KeyOwnerHelper.ThreeNode : KeyOwnerHelper.FiveNode;
        var writer = await (await cluster.ConnectClientAsync(anchor, cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var reader = await (await cluster.ConnectClientAsync(anchor, cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var rig = new Rig(cluster, fabric, probe, new FaultSchedule(nodes, anchor, replicas, seed), (writer, reader), (ring, FailoverSteps.KeysOf(ring, CacheName, Group, "probe", 1)[0]));
        var history = new List<(RegisterHistory History, string[] Keys)>();

        string Context()
        {
            return $"seed {seed.ToString(CultureInfo.InvariantCulture)}, steps: {string.Join(" -> ", trace)}";
        }

        try
        {
            for (var step = 1; step <= replicas * 4; step++)
                history.Add(await RunStepAsync(rig, step, trace, Context, cancellationToken));

            await FinishAsync(rig, nodes, history, Context, cancellationToken);
            var (acked, ambiguous, reads) = Totals(history);
            TestContext.Current?.Output.WriteLine($"{testName}: {trace.Count} steps, {acked} acknowledged and {ambiguous} ambiguous writes, {reads} reads; {Context()}");
        }
        catch (Exception exception) when (exception is not AssertionException)
        {
            throw new InvalidOperationException($"The fault schedule failed: {exception.Message}{Environment.NewLine}{Context()}", exception);
        }
    }

    /// <summary>Hands out the next fault, applies it under a register workload, and checks the recovery and the safety invariants.</summary>
    /// <param name="rig">The cluster, the schedule and the clients.</param>
    /// <param name="index">The position of the step, from one.</param>
    /// <param name="trace">Receives a line for the step before it is applied.</param>
    /// <param name="context">Describes the seed and the steps run so far, for a failure message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The history and the keys of the workload of the step.</returns>
    private static async Task<(RegisterHistory History, string[] Keys)> RunStepAsync(Rig rig, int index, List<string> trace, Func<string> context, CancellationToken cancellationToken)
    {
        var ledger = rig.Probe.Ledger(Group);
        var step = rig.Schedule.Next(ledger.Observe(rig.Schedule.Healthy()).NodeId);
        trace.Add($"{index}:{step.Kind}{(step.NodeId.Length == 0 ? string.Empty : " " + step.NodeId)}");
        var keys = FailoverSteps.KeysOf(rig.Keys.Ring, CacheName, Group, $"step{index.ToString(CultureInfo.InvariantCulture)}", Registers);
        var registers = new RegisterWorkload(rig.Clients.Writer, rig.Clients.Reader, keys) { Pace = Pace };
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = registers.RunUntilAsync(done.Task, TailOperations, cancellationToken);
        var safety = ledger.UntilAsync(() => running.IsCompleted, "the workload ends", cancellationToken);
        try
        {
            await ledger.UntilValueAsync(
                (rig.Clients.Reader, Key: keys[0]),
                static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
                "the workload makes progress",
                cancellationToken);

            var started = Stopwatch.GetTimestamp();
            await ApplyAsync(rig, step, cancellationToken);
            var attempts = new List<EventualAttempt>();
            var (read, elapsed) = await FailoverSteps.RecoverAsync(rig.Clients.Writer, rig.Keys.Probe, index, started, attempts, cancellationToken);
            var healthy = rig.Schedule.Healthy();
            _ = await rig.Probe.WaitForStableLeaderAsync(Group, healthy, FailoverSteps.Bound, cancellationToken);
            done.SetResult();
            await running;
            await safety;
            await FailoverSteps.ReadFinalAsync(registers.History, rig.Clients.Reader, keys, cancellationToken);
            _ = await GroupLogAudit.RunAsync(rig.Cluster.Cluster, Group, healthy, FailoverSteps.Bound, cancellationToken);

            var dump = context() + Eventually.Dump(attempts);
            _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, index)).Because(dump);
            _ = await Assert.That(elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump);
            _ = await Assert.That(registers.History.Check()).IsEmpty().Because(registers.History.Summary() + context());
            return (registers.History, keys);
        }
        catch
        {
            // Ends the workload, and waits for every started task, so none outlives the cluster; the first failure is the one rethrown.
            _ = done.TrySetResult();
            await Task.WhenAll(running, safety).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
    }

    /// <summary>Brings every node back and heals every link, waits until the group agrees, and reads every register of every step once more.</summary>
    /// <param name="rig">The cluster, the schedule and the clients.</param>
    /// <param name="nodes">The nodes of the cluster.</param>
    /// <param name="history">The history and the keys of every step's workload.</param>
    /// <param name="context">Describes the seed and the steps run, for a failure message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every history passed.</returns>
    private static async Task FinishAsync(Rig rig, string[] nodes, List<(RegisterHistory History, string[] Keys)> history, Func<string> context, CancellationToken cancellationToken)
    {
        foreach (var step in rig.Schedule.Drain())
            await ApplyAsync(rig, step, cancellationToken);

        _ = await rig.Probe.WaitForStableLeaderAsync(Group, nodes, FailoverSteps.Bound, cancellationToken);
        _ = await GroupLogAudit.RunAsync(rig.Cluster.Cluster, Group, nodes, FailoverSteps.Bound, cancellationToken);
        for (var i = 0; i < history.Count; i++)
        {
            var (registers, keys) = history[i];
            await FailoverSteps.ReadFinalAsync(registers, rig.Clients.Reader, keys, cancellationToken);
            _ = await Assert.That(registers.Check()).IsEmpty().Because($"step {i + 1}: {registers.Summary()} {context()}");
        }
    }

    private static async Task ApplyAsync(Rig rig, FaultStep step, CancellationToken cancellationToken)
    {
        switch (step.Kind)
        {
            case FaultKind.Stop:
                await rig.Cluster.StopNodeAsync(step.NodeId);
                break;
            case FaultKind.AbruptStop:
                await rig.Cluster.AbruptShutdownNodeAsync(step.NodeId);
                await rig.Cluster.StopNodeAsync(step.NodeId);
                break;
            case FaultKind.Restart:
                await rig.Cluster.RestartNodeAsync(step.NodeId, cancellationToken);
                break;
            case FaultKind.Isolate:
                await rig.Fabric.IsolateAsync(step.NodeId);
                break;
            case FaultKind.Heal:
                rig.Fabric.HealAll();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(step), step.Kind, "Unsupported enum value.");
        }
    }

    private static (int Acked, int Ambiguous, int Reads) Totals(List<(RegisterHistory History, string[] Keys)> history)
    {
        var (acked, ambiguous, reads) = (0, 0, 0);
        foreach (var (registers, _) in history)
        {
            var (writes, successful) = registers.StartedAfter(0);
            acked += writes;
            ambiguous += registers.AmbiguousWrites;
            reads += successful;
        }

        return (acked, ambiguous, reads);
    }

    /// <summary>What a schedule runs against.</summary>
    /// <param name="Cluster">The cluster.</param>
    /// <param name="Fabric">The fabric the nodes dial each other through.</param>
    /// <param name="Probe">The leader probe whose ledger checks election safety throughout.</param>
    /// <param name="Schedule">The schedule.</param>
    /// <param name="Clients">The caches the workloads write and read through, on the anchor node, each on a client of its own.</param>
    /// <param name="Keys">The key ring of the cluster, and the key the recovery probe writes.</param>
    private sealed record Rig(
        HostedCluster Cluster,
        PartitionFabric Fabric,
        ClusterLeaderProbe<ClusterStartOptions> Probe,
        FaultSchedule Schedule,
        (ICache<long> Writer, ICache<long> Reader) Clients,
        (KeyOwnerHelper Ring, string Probe) Keys);
}
