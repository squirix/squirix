using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// An RF=3 group owner compacts its group log once it reaches the entry threshold, and recovers from the compacted log. One test lets the
/// production maintenance timer compact; the others run the same maintenance step by hand, so no pass races the writes.
/// </summary>
public sealed class GroupLogCompactionTests : NodeIntegrationTestBase
{
    private const string CacheName = "group-log-compaction";
    private const string OwnerId = "node-a";
    private const int Threshold = 16;

    /// <summary>Bounds every wait; a healthy owner compacts within a few passes, so the bound only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>The group log maintenance interval of the test nodes that rely on the timer, far below the production default so the bound above holds.</summary>
    private static readonly TimeSpan MaintenanceInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>The maintenance interval of the test nodes that compact by hand; no timer pass fires within a test.</summary>
    private static readonly TimeSpan ManualMaintenanceInterval = TimeSpan.FromDays(1);

    /// <summary>After many overwrites the owner's group log holds a published snapshot and a bounded number of entries, compacted by the production maintenance timer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupLogStaysBoundedUnderOverwrites(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-bounded", true, MaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        _ = await OverwriteAsync(owner, Followers(cluster), 200, cancellationToken);

        var log = OwnerLog(owner);
        await AwaitCompactedAsync(log, Threshold * 2, cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);

        // The maintenance service keeps compacting until few entries remain, so the log, its file, and the readiness report are read
        // together as one state: the reads count only when the log did not move in between.
        var (retention, fileBytes, reported) = await ReadStableStateAsync(owner, log, cancellationToken);

        var logs = await DescribeLogsAsync(cluster, cancellationToken);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetSnapshotPath(owner.DataDir, OwnerId))).IsTrue().Because($"Compaction must publish group.snapshot; {logs}.");
        _ = await Assert.That(retention.RetainedEntries <= Threshold * 2).IsTrue().Because("The group log must keep at most twice the threshold.");
        _ = await Assert.That(fileBytes).IsEqualTo(retention.LogBytes);
        _ = await Assert.That(status.LastLogIndex >= 200).IsTrue().Because("Every overwrite must stay counted in the log index.");

        _ = await Assert.That(reported.GetProperty("snapshotIndex").GetUInt64()).IsEqualTo(retention.SnapshotIndex);
        _ = await Assert.That(reported.GetProperty("retainedEntries").GetInt32()).IsEqualTo(retention.RetainedEntries);
        _ = await Assert.That(reported.GetProperty("logBytes").GetInt64()).IsEqualTo(retention.LogBytes);
        var metrics = await HttpClient.GetStringAsync(new Uri(owner.Uri, "/metrics"), cancellationToken);
        _ = await Assert.That(metrics).Contains("squirix_replication_log_compactions_total{", StringComparison.Ordinal);
    }

    /// <summary>Under a steady write load the owner compacts repeatedly while writes are in flight, so the gated step fires between commits and its log stays bounded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="TimeoutException">Compaction did not fire the required number of times within the bound.</exception>
    [Test]
    public async Task SteadyLoadCompactsRepeatedly(CancellationToken cancellationToken)
    {
        const int writes = 300;
        const int requiredCompactions = 2;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-steady", true, ManualMaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var log = OwnerLog(owner);
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var compactions = new StrongBox<int>();
        using var writerDone = new CancellationTokenSource();

        // The maintenance step runs concurrently with the writer; a step the followers or the gate refuse is simply tried again.
        var compactor = CompactWhileAsync(owner, compactions, writerDone.Token, cancellationToken);

        var peak = 0;
        try
        {
            var started = Stopwatch.GetTimestamp();
            var i = 0;
            while (true)
            {
                i++;
                await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i }, cancellationToken);
                var retention = await log.GetRetentionAsync(cancellationToken);
                peak = Math.Max(peak, retention.RetainedEntries);
                if (i >= writes && Volatile.Read(ref compactions.Value) >= requiredCompactions)
                    break;

                if (Stopwatch.GetElapsedTime(started) >= Bound * 2)
                    throw new TimeoutException($"Compaction did not fire {requiredCompactions} times while writes went on; compactions: {Volatile.Read(ref compactions.Value)}; {await DescribeLogsAsync(cluster, cancellationToken)}.");
            }
        }
        finally
        {
            await writerDone.CancelAsync();
            await compactor;
        }

        var logs = await DescribeLogsAsync(cluster, cancellationToken);
        _ = await Assert.That(Volatile.Read(ref compactions.Value) >= requiredCompactions).IsTrue().Because($"Compaction must fire repeatedly while writes go on; compactions: {compactions.Value}; {logs}.");
        _ = await Assert.That(peak < writes / 2).IsTrue().Because($"The group log must stay bounded under load; peak retained entries: {peak}.");
    }

    /// <summary>
    /// A restarted owner recovers from its log compacted by the production maintenance step: the last value is readable, the next write lands right after the log, and a
    /// retry of an operation the snapshot covers replays its outcome without appending.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderRecoversAfterProductionCompaction(CancellationToken cancellationToken)
    {
        const string scope = "group-log-recovery";
        const int overwrites = 40;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true, ManualMaintenanceInterval), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[OwnerId], cancellationToken);
        var operations = await OverwriteAsync(cluster[OwnerId], Followers(cluster), overwrites, cancellationToken);
        await CompactAsync(cluster[OwnerId], cancellationToken);

        await cluster.StopNodeAsync(OwnerId);
        var restarted = await cluster.StartNodeAsync(OwnerId, Options(scope, false, ManualMaintenanceInterval), cancellationToken);
        var log = OwnerLog(restarted);
        var key = restarted.FindKeyOwnedBy(CacheName, OwnerId);
        await VerifyAsync(cluster, restarted, cancellationToken);
        var read = await restarted.GetCache<object?>(CacheName).GetValueAsync(CacheName, key, cancellationToken);
        var before = await log.GetStatusAsync(cancellationToken);
        var retention = await log.GetRetentionAsync(cancellationToken);

        // The retry takes the path of the original write, which reaches the group idempotency state the snapshot restored. The first
        // write found the key absent and went through as a conditional add, so the retried write is the second, a plain set.
        var cache = restarted.GetCache<object?>(CacheName);
        await cache.SetEntryAsync(operations[1], CacheName, key, Entry(2), cancellationToken);
        var retried = await log.GetStatusAsync(cancellationToken);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, Entry(overwrites + 1), cancellationToken);
        var next = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo($"value-{overwrites}");
        _ = await Assert.That(retention.SnapshotIndex > 0).IsTrue().Because("The restarted owner must recover from the compacted log.");
        _ = await Assert.That(retried.LastLogIndex).IsEqualTo(before.LastLogIndex).Because("A retry the snapshot covers must replay its outcome without appending.");
        _ = await Assert.That((next.LastLogIndex, next.CommitIndex)).IsEqualTo((before.LastLogIndex + 1, before.LastLogIndex + 1));
    }

    /// <summary>A follower that misses committed entries blocks the compaction: the owner keeps every entry that follower may still need.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DownFollowerBlocksCompaction(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-follower-down", true, ManualMaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        await cluster.StopNodeAsync("node-c");
        _ = await OverwriteAsync(owner, [], Threshold * 3, cancellationToken);

        var outcome = await ReplicaNodeCommitters.OwnCommitter(owner).CompactOwnedLogAsync(
            new ReplicaLogCompactionPolicy(long.MaxValue, Threshold),
            owner.GetRequiredService<IJournalCoordinator>(),
            cancellationToken);
        var retention = await OwnerLog(owner).GetRetentionAsync(cancellationToken);

        _ = await Assert.That(outcome == ReplicaLogCompactionOutcome.FollowerBehind || outcome == ReplicaLogCompactionOutcome.FollowerNotReady).IsTrue()
                        .Because($"A follower that misses entries must block the compaction; outcome: {outcome}.");
        _ = await Assert.That(retention.SnapshotIndex).IsEqualTo(0UL);
        _ = await Assert.That(retention.RetainedEntries >= Threshold * 3).IsTrue();
    }

    /// <summary>
    /// A follower stopped while the owner keeps writing is taken out of the quorum; once it is back it is caught up from the owner log
    /// and admitted again, and the compaction it blocked resumes, without a restart of the owner.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedFollowerRejoinsAndCompacts(CancellationToken cancellationToken)
    {
        const string scope = "group-log-follower-rejoin";
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true, ManualMaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        await cluster.StopNodeAsync("node-c");
        _ = await OverwriteAsync(owner, [], Threshold * 3, cancellationToken);

        var restarted = await cluster.StartNodeAsync("node-c", Options(scope, false, ManualMaintenanceInterval), cancellationToken);
        await CompactAsync(owner, cancellationToken);

        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [("node-c", restarted)], cancellationToken);
        _ = await Assert.That((await OwnerLog(owner).GetRetentionAsync(cancellationToken)).SnapshotIndex > 0).IsTrue();
    }

    /// <summary>A follower cut off from the owner by a network partition is caught up and admitted again once the partition heals, and the compaction resumes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IsolatedFollowerRejoinsAndCompacts(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            "node-c",
            Options("group-log-follower-isolated", true, ManualMaintenanceInterval, fabric),
            cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        await fabric.IsolateAsync("node-c");
        _ = await OverwriteAsync(owner, [], Threshold * 3, cancellationToken);

        fabric.HealAll();
        await CompactAsync(owner, cancellationToken);

        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [("node-c", cluster["node-c"])], cancellationToken);
        _ = await Assert.That((await OwnerLog(owner).GetRetentionAsync(cancellationToken)).SnapshotIndex > 0).IsTrue();
    }

    /// <summary>Describes the last log index each node holds of the owned group log, for failure messages.</summary>
    /// <param name="cluster">The running cluster.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The owner's and each follower's last log index of the owned group.</returns>
    private static async Task<string> DescribeLogsAsync(TestCluster<IntegrationStartOptions> cluster, CancellationToken cancellationToken)
    {
        var owner = await OwnerLog(cluster[OwnerId]).GetStatusAsync(cancellationToken);
        _ = cluster["node-b"].GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var first);
        _ = cluster["node-c"].GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var second);
        var b = await first!.GetStatusAsync(cancellationToken);
        var c = await second!.GetStatusAsync(cancellationToken);
        return $"owner last {owner.LastLogIndex} commit {owner.CommitIndex}, node-b last {b.LastLogIndex}, node-c last {c.LastLogIndex}";
    }

    private static (string Id, ITestNodeHost Host)[] Followers(TestCluster<IntegrationStartOptions> cluster) => [("node-b", cluster["node-b"]), ("node-c", cluster["node-c"])];

    private static NodeCacheEntry<object?> Entry(int version) => new() { Value = $"value-{version}", Version = version };

    private static IntegrationStartOptions Options(string scope, bool clean, TimeSpan maintenanceInterval, PartitionFabric? fabric = null) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        PartitionFabric = fabric,
        PersistenceOptions = new PersistenceOptions { JournalMaxSegmentMb = 64, ReplicaLogCompactionEntries = Threshold },
        ServicesConfigure = services => SetMaintenanceInterval(services, maintenanceInterval),
    };

    private static IFollowerLog OwnerLog(ITestNodeHost owner)
    {
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log);
        return log!;
    }

    /// <summary>Replaces the group log maintenance schedule.</summary>
    /// <param name="services">The node service collection.</param>
    /// <param name="interval">The delay between two maintenance passes.</param>
    private static void SetMaintenanceInterval(IServiceCollection services, TimeSpan interval)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = interval });
    }

    /// <summary>Overwrites one key owned by node-a, each write with a fresh operation identifier, and waits after the last write until the followers hold it.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="followers">The followers that must hold every write before the method returns; none when they need not keep up.</param>
    /// <param name="count">The number of overwrites.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The operation identifiers, in write order; write number i stores value-i at version i.</returns>
    private static async Task<string[]> OverwriteAsync(ITestNodeHost owner, (string Id, ITestNodeHost Host)[] followers, int count, CancellationToken cancellationToken)
    {
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var operations = new string[count];
        for (var i = 1; i <= count; i++)
        {
            operations[i - 1] = Guid.NewGuid().ToString("N");
            await cache.SetEntryAsync(operations[i - 1], CacheName, key, Entry(i), cancellationToken);
        }

        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, followers, cancellationToken);
        return operations;
    }

    /// <summary>Polls the owner's log until a snapshot is published and at most <paramref name="retained" /> entries remain.</summary>
    /// <param name="log">The owner's group log.</param>
    /// <param name="retained">The most entries the log may keep.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The maintenance timer did not compact the log within the bound.</exception>
    private static async Task AwaitCompactedAsync(IFollowerLog log, int retained, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var retention = await log.GetRetentionAsync(cancellationToken);
        while (retention.SnapshotIndex == 0 || retention.RetainedEntries > retained)
        {
            if (Stopwatch.GetElapsedTime(started) >= Bound)
                throw new TimeoutException($"The owner group log was not compacted within {Bound}; last retention {retention}.");

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
            retention = await log.GetRetentionAsync(cancellationToken);
        }
    }

    /// <summary>Runs the maintenance step of the owner repeatedly until told to stop, counting the passes that compacted.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="compactions">The counter of passes that compacted the log.</param>
    /// <param name="stop">Signals that the loop ends after its current pass.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task CompactWhileAsync(ITestNodeHost owner, StrongBox<int> compactions, CancellationToken stop, CancellationToken cancellationToken)
    {
        while (!stop.IsCancellationRequested)
        {
            var outcome = await CompactOnceAsync(owner, cancellationToken);
            if (outcome == ReplicaLogCompactionOutcome.Compacted)
                _ = Interlocked.Increment(ref compactions.Value);
            else
                await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>Runs the maintenance step of the owner once by hand: flushes the applied index, then compacts through the commit index.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The outcome of the compaction step.</returns>
    private static async Task<ReplicaLogCompactionOutcome> CompactOnceAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var committer = ReplicaNodeCommitters.OwnCommitter(owner);
        var journal = owner.GetRequiredService<IJournalCoordinator>();
        var policy = ReplicaLogCompactionPolicy.From(owner.GetRequiredService<PersistenceOptions>());
        await committer.FlushAppliedAsync(journal, cancellationToken);
        return await committer.CompactOwnedLogAsync(policy, journal, cancellationToken);
    }

    /// <summary>Runs the maintenance step of the owner by hand until it compacts the log.</summary>
    /// <param name="owner">The group owner, whose followers already hold its log.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The step did not compact the log within the bound.</exception>
    private static async Task CompactAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var outcome = await CompactOnceAsync(owner, cancellationToken);
            if (outcome == ReplicaLogCompactionOutcome.Compacted)
                return;

            if (Stopwatch.GetElapsedTime(started) >= Bound)
                throw new TimeoutException($"The compaction step did not compact the owner group log within {Bound}; last outcome {outcome}.");

            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>Verifies the restarted owner's replica slots, as its readiness service does, until every slot counts again.</summary>
    /// <param name="cluster">The running cluster.</param>
    /// <param name="owner">The restarted group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The owner did not verify every slot within the bound.</exception>
    private static async Task VerifyAsync(TestCluster<IntegrationStartOptions> cluster, ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var committer = ReplicaNodeCommitters.OwnCommitter(owner);
        while (await committer.VerifyReplicasAsync(cancellationToken) != ReplicaVerification.AllReady)
        {
            if (Stopwatch.GetElapsedTime(started) >= Bound)
                throw new TimeoutException($"The restarted owner did not verify every replica slot; {await DescribeLogsAsync(cluster, cancellationToken)}.");

            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>Reads the owner log retention, its file size, and the readiness report of the owned group as one state.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="log">The owner's group log.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The retention, the log file size, and the reported group, read while the retention did not change.</returns>
    /// <exception cref="TimeoutException">The log kept changing for the whole bound.</exception>
    private async Task<(FollowerLogRetention Retention, long FileBytes, JsonElement Reported)> ReadStableStateAsync(
        ITestNodeHost owner,
        IFollowerLog log,
        CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var before = await log.GetRetentionAsync(cancellationToken);
            var reported = await ReadyDetailsGroupAsync(owner, cancellationToken);
            var fileBytes = new FileInfo(GroupStoragePaths.GetLogPath(owner.DataDir, OwnerId)).Length;
            var after = await log.GetRetentionAsync(cancellationToken);
            if (before == after)
                return (after, fileBytes, reported);

            if (Stopwatch.GetElapsedTime(started) >= Bound)
                throw new TimeoutException($"The owner group log kept changing; last retention {after}.");

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>Reads the owned group's entry of <c language="csharp">/health/ready/details</c>.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The <c language="csharp">replicaGroups</c> element of the owned group.</returns>
    /// <exception cref="InvalidOperationException">The details report no entry for the owned group.</exception>
    private async Task<JsonElement> ReadyDetailsGroupAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var text = await HttpClient.GetStringAsync(new Uri(owner.Uri, "/health/ready/details"), cancellationToken);
        using var document = JsonDocument.Parse(text);
        foreach (var group in document.RootElement.GetProperty("replicaGroups").EnumerateArray())
        {
            if (string.Equals(group.GetProperty("groupId").GetString(), OwnerId, StringComparison.Ordinal))
                return group.Clone();
        }

        throw new InvalidOperationException("The readiness details report no entry for the owned group.");
    }
}
