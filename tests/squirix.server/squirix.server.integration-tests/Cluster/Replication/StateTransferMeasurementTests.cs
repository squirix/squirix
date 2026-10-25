using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// Measures the state transfer of a large replica group on an in-process three-node cluster. It runs only when
/// <see cref="StateTransferMeasurementSettings.KeysVariable" /> names a key count; otherwise it is skipped.
/// </summary>
/// <remarks>
/// <para>
/// One group is filled while a follower is down, so the owner log is pinned and grows; the owner then compacts past that follower, which returns
/// and is repaired by a state transfer while writers keep setting new keys. The report lists the phase timings, the write latency before and during
/// the transfer, the memory, disk and log figures, and the refusals writers saw.
/// </para>
/// <para>
/// Run it in a Release build with the key count in the environment, for example <c language="text">SQUIRIX_MEASURE_STATE_TRANSFER_KEYS=1000000</c>, and the
/// tree-node filter <c language="text">/*/*/StateTransferMeasurementTests/*</c>; the other variables of <see cref="StateTransferMeasurementSettings" /> shape the run.
/// </para>
/// </remarks>
[Property("Suite", "Measurement")]
public sealed class StateTransferMeasurementTests : NodeIntegrationTestBase
{
    private const string CacheName = StateTransferMeasurementSteps.CacheName;
    private const string OwnerId = StateTransferMeasurementSteps.OwnerId;
    private const string PeerId = "node-b";
    private const string LaggingId = "node-c";
    private const int MaxLiveKeys = 500_000;
    private const int RepeatKeys = 10_000;
    private const int BaselineKeys = 10_000;
    private const int UnreachableMb = 1_000_000;
    private const int LeaderJournalMb = 400_000;
    private const long BytesPerMegabyte = 1024L * 1024L;
    private const double MegabytesPerByte = 1.0 / BytesPerMegabyte;

    /// <summary>Bounds every wait of the run; a large group takes minutes, so the bound only catches a stuck run.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromHours(3);

    private static readonly TimeSpan ManualMaintenanceInterval = TimeSpan.FromDays(1);

    private static readonly string[] Nodes = [OwnerId, PeerId, LaggingId];

    private readonly StringBuilder _report = new();

    /// <summary>Measures one transfer of the configured size.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="SkipTestException">The measurement is not switched on.</exception>
    [Test]
    [Timeout(14_400_000)]
    public async Task MeasuresLargeStateTransfer(CancellationToken cancellationToken)
    {
        var settings = StateTransferMeasurementSettings.FromEnvironment() ?? NotSwitchedOn();
        try
        {
            await RunAsync(settings, cancellationToken);
        }
        finally
        {
            if (settings.ReportPath is { Length: > 0 } path)
                await File.AppendAllTextAsync(path, _report.ToString(), CancellationToken.None);
        }
    }

    private static StateTransferMeasurementSettings NotSwitchedOn() =>
        throw new SkipTestException($"The measurement is opt-in: set {StateTransferMeasurementSettings.KeysVariable} to the number of keys.");

    private static PersistenceOptions Persistence(int journalMb) => new()
    {
        JournalMaxSegmentMb = 64,
        JournalMaxTotalBytesMb = journalMb,
        ReplicaLogCompactionMb = UnreachableMb,
        ReplicaLogRetentionMaxMb = UnreachableMb,
        ReplicaLogCompactionEntries = int.MaxValue,
    };

    private static IntegrationStartOptions Options(bool clean, PersistenceOptions persistence, Action<IServiceCollection>? services, IReadOnlyDictionary<string, PressureOptions>? pressure) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = "state-transfer-measure",
        PersistenceOptions = persistence,
        MemoryPressureByNode = pressure,
        ServicesConfigure = serviceCollection =>
        {
            SetManualMaintenance(serviceCollection);
            services?.Invoke(serviceCollection);
        },
    };

    private static void SetManualMaintenance(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = ManualMaintenanceInterval });
    }

    private static async ValueTask<string> TraceAsync(Run run, CancellationToken cancellationToken)
    {
        var builder = new StringBuilder();
        _ = builder.Append(CultureInfo.InvariantCulture, $"t={Stopwatch.GetElapsedTime(run.Started).TotalSeconds:F0}s");
        foreach (var id in Nodes)
        {
            if (!run.Cluster.TryGetNode(id, out var node))
                continue;

            var status = await StateTransferMeasurementSteps.Log(node).GetStatusAsync(cancellationToken);
            _ = builder.Append(CultureInfo.InvariantCulture, $" {id}[last={status.LastLogIndex} commit={status.CommitIndex}]");
        }

        _ = builder.Append(CultureInfo.InvariantCulture, $" slots={run.Eligibility.StateFor(0)},{run.Eligibility.StateFor(1)},{run.Eligibility.StateFor(2)}");
        return builder.ToString();
    }

    private static double Megabytes(long bytes) => bytes * MegabytesPerByte;

    private static double Seconds(long timestamp, long since) => timestamp == 0 ? double.NaN : Stopwatch.GetElapsedTime(since, timestamp).TotalSeconds;

    private static double Elapsed(long from, long to) => from == 0 || to == 0 ? double.NaN : Stopwatch.GetElapsedTime(from, to).TotalSeconds;

    private async Task RunAsync(StateTransferMeasurementSettings settings, CancellationToken cancellationToken)
    {
        Line($"state transfer measurement: keys={settings.Keys} valueBytes={settings.ValueBytes} writers={settings.Writers} fillWriters={settings.FillWriters}");
        Line($"machine: {Environment.OSVersion} processors={Environment.ProcessorCount} gc={(System.Runtime.GCSettings.IsServerGC ? "server" : "workstation")}");
        await using var cluster = await StartClusterAsync(OwnerId, PeerId, LaggingId, Options(true, Persistence(LeaderJournalMb), null, null), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var liveKeys = Math.Min(settings.Keys, MaxLiveKeys);
        var run = new Run(cluster, settings, StateTransferMeasurementSteps.KeysOf(owner, settings.Keys + liveKeys + BaselineKeys), liveKeys);
        await using var sampler = new StateTransferSampler(
            Nodes,
            id => cluster.TryGetNode(id, out var node) ? node.GetRequiredService<IMemoryUsageAccounting>() : null,
            run.Directories[2],
            token => StateTransferMeasurementSteps.LagAsync(owner, run.Peer, token),
            token => TraceAsync(run, token));
        sampler.Start();
        await CheckpointAsync("start", run, cancellationToken);
        await BaselineAsync(run, cancellationToken);
        await FillAsync(run, (0, settings.Keys), cancellationToken);
        var compaction = await StateTransferMeasurementSteps.CompactPastFollowerAsync(owner, cancellationToken);
        Line($"compaction past the follower: {compaction}");
        await CheckpointAsync("after compaction", run, cancellationToken);
        _ = await TransferAsync(run, sampler, cancellationToken);
        await CheckpointAsync("after transfer", run, cancellationToken);
        await AfterAsync(run, cancellationToken);
        for (var repeat = 2; repeat <= settings.Repeats; repeat++)
            await RepeatAsync(run, repeat, sampler, cancellationToken);

        var (ownerEntries, followerEntries, mismatches) = await StateTransferMeasurementSteps.VerifyAsync(run.Owner, run.Cluster[LaggingId], run.Keys, run.Settings.Keys, cancellationToken);
        Line($"verification: owner entries={ownerEntries} follower entries={followerEntries} sampled value mismatches={mismatches}");

        _ = await Assert.That(followerEntries).IsEqualTo(StateTransferMeasurementSteps.EntryCount(run.Peer));
        _ = await Assert.That(mismatches).IsEqualTo(0);
    }

    private async Task BaselineAsync(Run run, CancellationToken cancellationToken)
    {
        var settings = run.Settings;
        var load = new StateTransferWriteLoad(run.Cache, CacheName, run.Keys, (settings.Keys + run.LiveKeys, BaselineKeys), (settings.Writers, settings.ValueBytes, 0), cancellationToken);
        load.Start();
        await Task.Delay(settings.Baseline, TimeProvider.System, cancellationToken);
        Line($"baseline (all members up, {settings.Writers} writers, {settings.Baseline.TotalSeconds:F0} s): {(await load.StopAsync()).Describe()}");
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(run.Owner, OwnerId, [(PeerId, run.Peer), (LaggingId, run.Cluster[LaggingId])], cancellationToken);
        await CheckpointAsync("after baseline", run, cancellationToken);
    }

    private async Task FillAsync(Run run, (int Start, int Count) range, CancellationToken cancellationToken)
    {
        var settings = run.Settings;
        await run.Cluster.StopNodeAsync(LaggingId);
        var load = new StateTransferWriteLoad(run.Cache, CacheName, run.Keys, (range.Start, range.Count), (settings.FillWriters, settings.ValueBytes, range.Count), cancellationToken);
        var started = Stopwatch.GetTimestamp();
        load.Start();
        var result = await load.CompleteAsync();
        Line($"fill (node-c down, {settings.FillWriters} writers, {Stopwatch.GetElapsedTime(started).TotalSeconds:F1} s): {result.Describe()}");
        await (Eligibility: run.Eligibility, Slot: run.Slot).WaitUntilValueAsync(
            static (state, _) => ValueTask.FromResult(!state.Eligibility.CanCountInWriteQuorum(state.Slot)),
            Bound,
            TimeProvider.System,
            cancellationToken);
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(run.Owner, OwnerId, [(PeerId, run.Peer)], cancellationToken);
        await CheckpointAsync("pinned (node-c down, owner log held)", run, cancellationToken);
    }

    private async Task<ITestNodeHost> TransferAsync(Run run, StateTransferSampler sampler, CancellationToken cancellationToken)
    {
        var settings = run.Settings;
        _ = sampler.TakePeaks();
        _ = sampler.TakeTimeline();
        var timings = new StateTransferTimings();
        var live = new StateTransferWriteLoad(run.Cache, CacheName, run.Keys, (settings.Keys, run.LiveKeys), (settings.Writers, settings.ValueBytes, 0), cancellationToken);
        var pressure = settings.FollowerMaxCacheMb is { } mb
            ? new Dictionary<string, PressureOptions>(StringComparer.Ordinal) { [LaggingId] = new() { MaxEstimatedCacheBytes = mb * BytesPerMegabyte } }
            : null;
        var started = Stopwatch.GetTimestamp();
        Line(string.Create(CultureInfo.InvariantCulture, $"transfer window starts at t={Stopwatch.GetElapsedTime(run.Started).TotalSeconds:F0}s of the run"));
        live.Start();
        var restarted = await run.Cluster.StartNodeAsync(
            LaggingId,
            Options(false, Persistence(settings.FollowerJournalMb ?? LeaderJournalMb), timings.Configure, pressure),
            cancellationToken);
        var bootSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        var readySeconds = double.NaN;
        var gcBefore = (GC.GetTotalPauseDuration(), GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        try
        {
            await (Eligibility: run.Eligibility, Slot: run.Slot).WaitUntilValueAsync(
                static (state, _) => ValueTask.FromResult(state.Eligibility.StateFor(state.Slot) == ReplicaParticipantState.Ready),
                settings.ReadyTimeout,
                TimeProvider.System,
                cancellationToken);
            readySeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        }
        finally
        {
            var during = await live.StopAsync();
            Line($"during the transfer ({settings.Writers} writers, window {Stopwatch.GetElapsedTime(started).TotalSeconds:F1} s): {during.Describe()}");
            var peaks = sampler.TakePeaks();
            ReportTransfer(timings, in peaks, (started, bootSeconds, readySeconds));
            Line(string.Create(
                CultureInfo.InvariantCulture,
                $"gc in the window: pause={(GC.GetTotalPauseDuration() - gcBefore.Item1).TotalSeconds:F2}s gen0={GC.CollectionCount(0) - gcBefore.Item2} gen1={GC.CollectionCount(1) - gcBefore.Item3} gen2={GC.CollectionCount(2) - gcBefore.Item4} longestSamplerGap={peaks.MaxTickGapMs}ms"));
            foreach (var line in sampler.TakeTimeline())
                Line($"timeline {line}");

            await ReportMetricsAsync(run, cancellationToken);
            await CheckpointAsync("end of the wait", run, cancellationToken);
        }

        await ReplicaGroupFollowers.AwaitCaughtUpAsync(run.Owner, OwnerId, [(PeerId, run.Peer), (LaggingId, restarted)], cancellationToken);
        return restarted;
    }

    private void ReportTransfer(StateTransferTimings timings, in StateTransferSampler.Peaks peaks, (long Started, double Boot, double Ready) window)
    {
        var culture = CultureInfo.InvariantCulture;
        Line(string.Create(
            culture,
            $"transfer: entered={Seconds(timings.EnteredTimestamp, window.Started):F1}s sealed={Seconds(timings.SealedTimestamp, window.Started):F1}s ready={window.Ready:F1}s (node boot {window.Boot:F1}s) wipeSteps={timings.WipeSteps}"));
        Line(string.Create(
            culture,
            $"transfer on the follower: duration={Elapsed(timings.EnteredTimestamp, timings.SealedTimestamp):F1}s writes={Elapsed(timings.FirstWriteTimestamp, timings.LastWriteTimestamp):F1}s entries={timings.Entries} keyAndPayloadMB={Megabytes(timings.Bytes):F0}"));
        Line(string.Create(
            culture,
            $"peaks in the window: workingSetMB={Megabytes(peaks.WorkingSetBytes):F0} managedHeapMB={Megabytes(peaks.ManagedBytes):F0} gcCommittedMB={Megabytes(peaks.CommittedBytes):F0} estimatedCacheMB node-a={Megabytes(peaks.NodeEstimatedBytes[0]):F0} node-b={Megabytes(peaks.NodeEstimatedBytes[1]):F0} node-c={Megabytes(peaks.NodeEstimatedBytes[2]):F0} node-cDiskMB={Megabytes(peaks.WatchedDiskBytes):F0} node-bMaxLagEntries={peaks.MaxLagEntries}"));
    }

    private async Task RepeatAsync(Run run, int number, StateTransferSampler sampler, CancellationToken cancellationToken)
    {
        Line($"repeat {number}: the follower falls behind again and is repaired by another transfer on the same cluster");
        await FillAsync(run, (run.Settings.Keys, Math.Min(RepeatKeys, run.LiveKeys)), cancellationToken);
        Line($"compaction past the follower: {await StateTransferMeasurementSteps.CompactPastFollowerAsync(run.Owner, cancellationToken)}");
        _ = await TransferAsync(run, sampler, cancellationToken);
        await CheckpointAsync($"after transfer {number}", run, cancellationToken);
    }

    private async Task AfterAsync(Run run, CancellationToken cancellationToken)
    {
        var settings = run.Settings;
        var load = new StateTransferWriteLoad(run.Cache, CacheName, run.Keys, (settings.Keys, run.LiveKeys), (settings.Writers, settings.ValueBytes, 0), cancellationToken);
        load.Start();
        await Task.Delay(settings.Baseline, TimeProvider.System, cancellationToken);
        Line($"after the transfer (all members up, {settings.Writers} writers, {settings.Baseline.TotalSeconds:F0} s): {(await load.StopAsync()).Describe()}");
    }

    private async Task ReportMetricsAsync(Run run, CancellationToken cancellationToken)
    {
        var metrics = await HttpClient.GetStringAsync(new Uri(run.Owner.Uri, "/metrics"), cancellationToken);
        using var reader = new StringReader(metrics);
        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (line.StartsWith("squirix_replication_state_transfers_total", StringComparison.Ordinal)
                || line.StartsWith("squirix_memory_rejections_total", StringComparison.Ordinal)
                || line.StartsWith("squirix_replication_log_compactions_total", StringComparison.Ordinal))
                Line($"owner metric: {line.TrimEnd()}");
        }
    }

    private async Task CheckpointAsync(string name, Run run, CancellationToken cancellationToken)
    {
        var builder = new StringBuilder();
        _ = builder.Append(CultureInfo.InvariantCulture, $"checkpoint {name}: liveManagedMB={Megabytes(StateTransferMeasurementSteps.LiveManagedBytes()):F0} workingSetMB={Megabytes(Environment.WorkingSet):F0}");
        for (var i = 0; i < Nodes.Length; i++)
        {
            var disk = Megabytes(StateTransferSampler.DirectoryBytes(run.Directories[i]));
            if (!run.Cluster.TryGetNode(Nodes[i], out var node))
            {
                _ = builder.Append(CultureInfo.InvariantCulture, $" | {Nodes[i]} down diskMB={disk:F0}");
                continue;
            }

            var status = await StateTransferMeasurementSteps.Log(node).GetStatusAsync(cancellationToken);
            _ = builder.Append(CultureInfo.InvariantCulture, $" | {Nodes[i]} log last={status.LastLogIndex} commit={status.CommitIndex} applied={status.LastAppliedIndex} readiness={status.Readiness} member={status.MemberState}");
            var accounting = node.GetRequiredService<IMemoryUsageAccounting>();
            _ = builder.Append(CultureInfo.InvariantCulture, $" | {Nodes[i]} entries={accounting.ReadEntryCount()} estimatedMB={Megabytes(accounting.ReadEstimatedBytes()):F0} diskMB={disk:F0}");
            if (i == 0)
            {
                var retention = await StateTransferMeasurementSteps.Log(node).GetRetentionAsync(cancellationToken);
                _ = builder.Append(CultureInfo.InvariantCulture, $" groupLogMB={Megabytes(retention.LogBytes):F0} retainedEntries={retention.RetainedEntries}");
            }
        }

        _ = builder.Append(CultureInfo.InvariantCulture, $" | owner view of slots: {run.Eligibility.StateFor(0)}, {run.Eligibility.StateFor(1)}, {run.Eligibility.StateFor(2)} (node-c is slot {run.Slot})");
        Line(builder.ToString());
    }

    private void Line(string text)
    {
        Console.WriteLine(text);
        _ = _report.AppendLine(text);
    }

    /// <summary>The cluster of a run with the handles every phase uses.</summary>
    private sealed class Run
    {
        internal Run(TestCluster<IntegrationStartOptions> cluster, StateTransferMeasurementSettings settings, string[] keys, int liveKeys)
        {
            Cluster = cluster;
            Settings = settings;
            Keys = keys;
            LiveKeys = liveKeys;
            Owner = cluster[OwnerId];
            Peer = cluster[PeerId];
            Cache = Owner.GetCache<object?>(CacheName);
            Eligibility = Owner.GetRequiredService<ReplicaGroupRegistry>().EligibilityFor(OwnerId);
            Slot = StateTransferMeasurementSteps.SlotOf(Owner, LaggingId);
            Started = Stopwatch.GetTimestamp();
            Directories = [cluster[OwnerId].DataDir, cluster[PeerId].DataDir, cluster[LaggingId].DataDir];
        }

        internal ILogicalNamespacedCache<object?> Cache { get; }

        internal TestCluster<IntegrationStartOptions> Cluster { get; }

        /// <summary>Gets the data directories in the order of the node identifiers.</summary>
        internal string[] Directories { get; }

        internal ReplicaEligibility Eligibility { get; }

        internal string[] Keys { get; }

        internal int LiveKeys { get; }

        internal ITestNodeHost Owner { get; }

        internal ITestNodeHost Peer { get; }

        internal StateTransferMeasurementSettings Settings { get; }

        internal int Slot { get; }

        /// <summary>Gets the time the run started.</summary>
        internal long Started { get; }
    }
}
