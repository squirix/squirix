using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>An RF=3 group owner compacts its group log in production once it reaches the entry threshold, and recovers from the compacted log.</summary>
public sealed class GroupLogCompactionTests : NodeIntegrationTestBase
{
    private const string CacheName = "group-log-compaction";
    private const string OwnerId = "node-a";
    private const int Threshold = 16;

    /// <summary>Bounds every wait for the maintenance service; a healthy owner compacts within a few passes.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>The group log maintenance interval of the test nodes, far below the production default so the bound above holds.</summary>
    private static readonly TimeSpan MaintenanceInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>After many overwrites the owner's group log holds a published snapshot and a bounded number of entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupLogStaysBoundedUnderOverwrites(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-bounded", true), cancellationToken);
        var owner = cluster[OwnerId];
        _ = await OverwriteAsync(owner, Followers(cluster), 200, cancellationToken);

        var log = OwnerLog(owner);
        _ = await AwaitCompactedAsync(log, Threshold * 2, cancellationToken);
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

    /// <summary>Under a steady write load the owner keeps compacting while writes go on, so the gated step fires between commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SteadyLoadCompactsRepeatedly(CancellationToken cancellationToken)
    {
        const int writes = 300;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-steady", true), cancellationToken);
        var owner = cluster[OwnerId];
        var log = OwnerLog(owner);
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var followers = Followers(cluster);
        var snapshots = new HashSet<ulong>();
        var peak = 0;
        for (var i = 1; i <= writes; i++)
        {
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i }, cancellationToken);

            // Compaction never passes a follower that is behind, and a follower that misses an entry never catches up: each write is
            // received by every follower before the next one starts, so the load cannot leave a follower behind.
            await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, followers, cancellationToken);
            var retention = await log.GetRetentionAsync(cancellationToken);
            peak = Math.Max(peak, retention.RetainedEntries);
            if (retention.SnapshotIndex > 0)
                _ = snapshots.Add(retention.SnapshotIndex);
        }

        var logs = await DescribeLogsAsync(cluster, cancellationToken);
        _ = await Assert.That(snapshots.Count >= 2).IsTrue().Because($"Compaction must fire repeatedly while writes go on; snapshot indexes seen: {snapshots.Count}; {logs}.");
        _ = await Assert.That(peak < writes / 2).IsTrue().Because($"The group log must stay bounded under load; peak retained entries: {peak}.");
    }

    /// <summary>
    /// A restarted owner recovers from its compacted log: the last value is readable, the next write lands right after the log, and a
    /// retry of an operation the snapshot covers replays its outcome without appending.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderRecoversAfterProductionCompaction(CancellationToken cancellationToken)
    {
        const string scope = "group-log-recovery";
        const int overwrites = 40;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        var operations = await OverwriteAsync(cluster[OwnerId], Followers(cluster), overwrites, cancellationToken);
        _ = await AwaitCompactedAsync(OwnerLog(cluster[OwnerId]), Threshold, cancellationToken);

        await cluster.StopNodeAsync(OwnerId);
        var restarted = await cluster.StartNodeAsync(OwnerId, Options(scope, false), cancellationToken);
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
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-follower-down", true), cancellationToken);
        var owner = cluster[OwnerId];
        await cluster.StopNodeAsync("node-c");
        _ = await OverwriteAsync(owner, [], Threshold * 3, cancellationToken);

        var outcome = await owner.GetRequiredService<ReplicaGroupCommitter>().CompactOwnedLogAsync(
            new ReplicaLogCompactionPolicy(long.MaxValue, Threshold),
            owner.GetRequiredService<IJournalCoordinator>(),
            cancellationToken);
        var retention = await OwnerLog(owner).GetRetentionAsync(cancellationToken);

        _ = await Assert.That(outcome == ReplicaLogCompactionOutcome.FollowerBehind || outcome == ReplicaLogCompactionOutcome.FollowerNotReady).IsTrue()
                        .Because($"A follower that misses entries must block the compaction; outcome: {outcome}.");
        _ = await Assert.That(retention.SnapshotIndex).IsEqualTo(0UL);
        _ = await Assert.That(retention.RetainedEntries >= Threshold * 3).IsTrue();
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

    private static ITestNodeHost[] Followers(TestCluster<IntegrationStartOptions> cluster) => [cluster["node-b"], cluster["node-c"]];

    private static NodeCacheEntry<object?> Entry(int version) => new() { Value = $"value-{version}", Version = version };

    private static IntegrationStartOptions Options(string scope, bool clean) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        PersistenceOptions = new PersistenceOptions { JournalMaxSegmentMb = 64, ReplicaLogCompactionEntries = Threshold },
        ServicesConfigure = ShortenMaintenance,
    };

    private static IFollowerLog OwnerLog(ITestNodeHost owner)
    {
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log);
        return log!;
    }

    /// <summary>Replaces the group log maintenance schedule with <see cref="MaintenanceInterval" />.</summary>
    /// <param name="services">The node service collection.</param>
    private static void ShortenMaintenance(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = MaintenanceInterval });
    }

    /// <summary>Overwrites one key owned by node-a, each write with a fresh operation identifier, and waits after each write until the followers hold it.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="followers">The followers that must receive each write before the next one starts; none when they need not keep up.</param>
    /// <param name="count">The number of overwrites.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The operation identifiers, in write order; write number i stores value-i at version i.</returns>
    private static async Task<string[]> OverwriteAsync(ITestNodeHost owner, ITestNodeHost[] followers, int count, CancellationToken cancellationToken)
    {
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var operations = new string[count];
        for (var i = 1; i <= count; i++)
        {
            operations[i - 1] = Guid.NewGuid().ToString("N");
            await cache.SetEntryAsync(operations[i - 1], CacheName, key, Entry(i), cancellationToken);
            await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, followers, cancellationToken);
        }

        return operations;
    }

    /// <summary>Polls the owner's log until a snapshot is published and at most <paramref name="retained" /> entries remain, or the bound elapses.</summary>
    /// <param name="log">The owner's group log.</param>
    /// <param name="retained">The most entries the log may keep.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last observed retention; the caller asserts on it.</returns>
    private static async Task<FollowerLogRetention> AwaitCompactedAsync(IFollowerLog log, int retained, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var retention = await log.GetRetentionAsync(cancellationToken);
        while ((retention.SnapshotIndex == 0 || retention.RetainedEntries > retained) && Stopwatch.GetElapsedTime(started) < Bound)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
            retention = await log.GetRetentionAsync(cancellationToken);
        }

        return retention;
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
        var committer = owner.GetRequiredService<ReplicaGroupCommitter>();
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
