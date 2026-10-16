using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A leader cut off from both followers fails closed: it serves no current read and acknowledges no write, while the majority elects a leader
/// in a later term and keeps serving through the SDK. Once the links heal, the former leader follows the majority, no acknowledged write is
/// lost, reads stay linearizable, and the three members retain one committed log.
/// </summary>
public sealed class PartitionFailoverTests : EndToEndTestBase
{
    private const string CacheName = "partition-failover";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>The value of a register the readers must see before the fault starts, so the fault hits a running workload.</summary>
    private const long Progress = 5;

    /// <summary>The writes, and the reads, each register runs after the recovery probe succeeded, so the workload covers the recovered group.</summary>
    private const int TailOperations = 10;

    /// <summary>
    /// The leader of the group is cut off from both followers under a register workload through the majority. A client of the cut-off node
    /// gets no acknowledged write and no current read, right after the cut and once the majority leads; after the links heal, the node serves
    /// what the majority committed and the write it was refused is gone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task IsolatedLeaderFailsClosed(CancellationToken cancellationToken)
    {
        const string testName = nameof(IsolatedLeaderFailsClosed);
        await using var fabric = new PartitionFabric();
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName, fabric: fabric), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var majority = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var isolated = await cluster.GetCacheAsync<long>(CacheName, former, cancellationToken);
        var writer = await cluster.GetCacheAsync<long>(CacheName, majority[0], cancellationToken);
        var reader = await cluster.GetCacheAsync<long>(CacheName, majority[1], cancellationToken);
        var keys = FailoverSteps.KeysOf(CacheName, Group, "register", 4);
        var singles = (
            Stale: FailoverSteps.KeysOf(CacheName, Group, "stale", 1)[0],
            Lost: FailoverSteps.KeysOf(CacheName, Group, "lost", 1)[0],
            Probe: FailoverSteps.KeysOf(CacheName, Group, "probe", 1)[0]);
        await isolated.SetAsync(singles.Stale, 1L, cancellationToken: cancellationToken);

        var scene = new Scene(fabric, probe, (former, formerTerm), (isolated, writer, reader), keys, singles);
        var registers = new RegisterWorkload(writer, reader, keys);
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = registers.RunUntilAsync(recovered.Task, TailOperations, cancellationToken);
        var safety = probe.Ledger(Group).UntilAsync(() => running.IsCompleted, "the workload ends", cancellationToken);
        try
        {
            var cut = await CutOffAsync(scene, recovered, cancellationToken);
            await running;
            await safety;

            var healed = await HealAsync(cluster, scene, registers.History, cut.Attempts, cancellationToken);

            var dump = Eventually.Dump(cut.Attempts);
            _ = await Assert.That(IsRefusal(cut.Write)).IsTrue().Because($"the cut-off node must refuse the write, got {Describe(cut.Write)}");
            _ = await Assert.That(cut.HeldAuthority).IsTrue().Because("the cut-off node must still believe it leads when the read is issued, or the read does not cover that window");
            _ = await Assert.That(IsRefusal(cut.Read)).IsTrue().Because($"the cut-off node must refuse the read, got {Describe(cut.Read)}");
            _ = await Assert.That(IsRefusal(cut.DeposedRead)).IsTrue().Because($"the deposed node must refuse the read, got {Describe(cut.DeposedRead)}");
            _ = await Assert.That(cut.ProbeRead).IsEqualTo(new CacheValueResult<long>(true, 1L)).Because(dump);
            _ = await Assert.That(cut.Elapsed).IsLessThanOrEqualTo(FailoverSteps.RecoveryBound).Because(dump);
            _ = await Assert.That(majority).Contains(cut.Leader.NodeId);
            _ = await Assert.That(cut.Leader.Term).IsGreaterThan(formerTerm);
            _ = await Assert.That(healed.RejoinedTerm).IsGreaterThanOrEqualTo(cut.Leader.Term);
            _ = await Assert.That(registers.History.Check()).IsEmpty().Because(registers.History.Summary());
            _ = await Assert.That(healed.LostRead).IsEqualTo(new CacheValueResult<long>(false, 0L)).Because("a write the cut-off leader refused must not surface after the links heal");
            _ = await Assert.That(healed.StaleRead).IsEqualTo(new CacheValueResult<long>(true, 2L)).Because(dump);
            _ = await Assert.That(healed.ClientEntries).IsGreaterThan(0);
        }
        catch
        {
            // Ends the workload, and waits for every started task, so none outlives the cluster; the first failure is the one rethrown.
            _ = recovered.TrySetResult();
            await Task.WhenAll(running, safety).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
    }

    /// <summary>
    /// Cuts the leader off once the workload made progress, probes the cut-off node right after the cut and once the majority leads, waits
    /// for the majority to serve a write and a read, and then lets the workload finish its tail.
    /// </summary>
    /// <param name="scene">The cluster, the clients and the keys of the test.</param>
    /// <param name="recovered">Completed once the majority serves, so the workload ends after its tail.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the cut recorded.</returns>
    private static async Task<Cut> CutOffAsync(Scene scene, TaskCompletionSource recovered, CancellationToken cancellationToken)
    {
        await scene.Probe.Ledger(Group).UntilValueAsync(
            (scene.Clients.Reader, Key: scene.Keys[0]),
            static async (s, token) => (await s.Reader.GetValueAsync(s.Key, token)).Value >= Progress,
            "the workload makes progress",
            cancellationToken);

        var started = Stopwatch.GetTimestamp();
        await scene.Fabric.IsolateAsync(scene.Former.NodeId);
        var attempts = new List<EventualAttempt>();
        var recovery = FailoverSteps.RecoverAsync(scene.Clients.Writer, scene.Singles.Probe, 1L, started, attempts, cancellationToken);

        // The cut-off node may still hold authority right after the cut, so it must refuse by itself: no quorum confirms its read or commits its write.
        // Both calls start before either is awaited, so the read is issued while the node can still believe it leads.
        var (holder, _) = scene.Probe.Ledger(Group).Observe([scene.Former.NodeId]);
        var writing = scene.Clients.Isolated.SetAsync(scene.Singles.Lost, 99L, cancellationToken: cancellationToken);
        var reading = scene.Clients.Isolated.GetValueAsync(scene.Singles.Stale, cancellationToken);
        var write = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(writing);
        var read = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(reading);
        var heldAuthority = string.Equals(holder, scene.Former.NodeId, StringComparison.Ordinal);
        var (probeRead, elapsed) = await recovery;
        var leader = await scene.Probe.WaitForNewLeaderAsync(Group, scene.Former.Term, FailoverSteps.Bound, cancellationToken);

        await Eventually.SucceedsAsync(
            (Cache: scene.Clients.Writer, Key: scene.Singles.Stale),
            static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token),
            FailoverSteps.Bound,
            attempts,
            cancellationToken);
        var deposedRead = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(scene.Clients.Isolated.GetValueAsync(scene.Singles.Stale, cancellationToken));
        recovered.SetResult();
        return new Cut(write, read, heldAuthority, deposedRead, probeRead, elapsed, leader, attempts);
    }

    /// <summary>
    /// Heals the links and waits until the three members follow one leader, reads every register through the former leader, reads the keys of
    /// the refused and the overwritten write, and audits the committed logs.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="scene">The cluster, the clients and the keys of the test.</param>
    /// <param name="history">The history of the workload.</param>
    /// <param name="attempts">Receives one entry per attempt of the reads.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the healed cluster reports.</returns>
    private static async Task<Healed> HealAsync(HostedCluster cluster, Scene scene, RegisterHistory history, List<EventualAttempt> attempts, CancellationToken cancellationToken)
    {
        scene.Fabric.HealAll();
        var (_, rejoinedTerm) = await scene.Probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        await FailoverSteps.ReadFinalAsync(history, scene.Clients.Isolated, scene.Keys, cancellationToken);
        var lost = await Eventually.SucceedsAsync((Cache: scene.Clients.Isolated, Key: scene.Singles.Lost), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var stale = await Eventually.SucceedsAsync((Cache: scene.Clients.Isolated, Key: scene.Singles.Stale), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        return new Healed(rejoinedTerm, lost, stale, report.ClientEntries);
    }

    private static string Describe(Exception exception) => exception is RpcException rpc ? $"{nameof(RpcException)} {rpc.StatusCode} '{rpc.Status.Detail}'" : exception.GetType().Name;

    /// <summary>Tells whether a failure is a refusal the client can act on: an unavailable, timed out or shed call, or a commit with an unknown outcome.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="true" /> for a refusal; otherwise <see langword="false" />.</returns>
    private static bool IsRefusal(Exception exception) =>
        exception is CommitOutcomeUnknownException or RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.ResourceExhausted };

    /// <summary>The cluster, the clients and the keys of the test.</summary>
    /// <param name="Fabric">The fabric the nodes dial each other through.</param>
    /// <param name="Probe">The leader probe of the cluster.</param>
    /// <param name="Former">The leader that is cut off, and its term.</param>
    /// <param name="Clients">The cache of the cut-off node, and the caches the workload writes and reads through on the majority.</param>
    /// <param name="Keys">The register keys.</param>
    /// <param name="Singles">The keys of the stale read, of the refused write and of the recovery probe.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Scene(
        PartitionFabric Fabric,
        ClusterLeaderProbe<ClusterStartOptions> Probe,
        (string NodeId, ulong Term) Former,
        (ICache<long> Isolated, ICache<long> Writer, ICache<long> Reader) Clients,
        string[] Keys,
        (string Stale, string Lost, string Probe) Singles);

    /// <summary>What the healed cluster reports.</summary>
    /// <param name="RejoinedTerm">The term of the leader the three members follow.</param>
    /// <param name="LostRead">The value of the key whose write the cut-off node refused, read through that node.</param>
    /// <param name="StaleRead">The value of the key the majority overwrote, read through the former leader.</param>
    /// <param name="ClientEntries">The number of client entries in the committed log the members share.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Healed(ulong RejoinedTerm, CacheValueResult<long> LostRead, CacheValueResult<long> StaleRead, int ClientEntries);

    /// <summary>What the cut recorded.</summary>
    /// <param name="Write">How the cut-off node refused the write.</param>
    /// <param name="Read">How the cut-off node refused the read right after the cut.</param>
    /// <param name="HeldAuthority">Whether the cut-off node still held authority over the group when the read and the write were issued.</param>
    /// <param name="DeposedRead">How the cut-off node refused the read once the majority led.</param>
    /// <param name="ProbeRead">The value the recovery probe read back through the majority.</param>
    /// <param name="Elapsed">The time from the cut until the probe read its value back.</param>
    /// <param name="Leader">The leader the majority elected, and its term.</param>
    /// <param name="Attempts">The attempts of the probe and of the later waits.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Cut(
        Exception Write,
        Exception Read,
        bool HeldAuthority,
        Exception DeposedRead,
        CacheValueResult<long> ProbeRead,
        TimeSpan Elapsed,
        (string NodeId, ulong Term) Leader,
        List<EventualAttempt> Attempts);
}
