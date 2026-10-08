using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// The followers of an RF=3 group compact their copy of the group log through the applied index they persisted, keep the committed
/// entries above it, and recover from the compacted log.
/// </summary>
public sealed class FollowerLogCompactionTests : NodeIntegrationTestBase
{
    private const string CacheName = "follower-log-compaction";
    private const string FollowerId = "node-b";
    private const string OwnerId = "node-a";
    private const int Threshold = 16;

    /// <summary>Bounds every wait; a healthy follower compacts within a few passes, so the bound only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>The maintenance interval of the test nodes that rely on the timer, far below the production default so the bound above holds.</summary>
    private static readonly TimeSpan MaintenanceInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>The maintenance interval of the test nodes that compact by hand; no timer pass fires within a test.</summary>
    private static readonly TimeSpan ManualMaintenanceInterval = TimeSpan.FromDays(1);

    /// <summary>
    /// While the owner keeps writing, a follower's production maintenance timer compacts its copy of the group log repeatedly through the
    /// applied index it persisted, so the follower log stays bounded.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="TimeoutException">The follower did not compact the required number of times within the bound.</exception>
    [Test]
    public async Task FollowerLogCompactsUnderSteadyWrites(CancellationToken cancellationToken)
    {
        const int writes = 200;
        const int requiredCompactions = 2;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("follower-log-steady", true, MaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var follower = GroupLog(cluster[FollowerId]);
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var compactions = 0;
        var snapshotIndex = 0UL;
        var started = Stopwatch.GetTimestamp();
        for (var i = 1; i <= writes || compactions < requiredCompactions; i++)
        {
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, Entry(i), cancellationToken);
            var retention = await follower.GetRetentionAsync(cancellationToken);
            if (retention.SnapshotIndex != snapshotIndex)
            {
                compactions++;
                snapshotIndex = retention.SnapshotIndex;
            }

            if (Stopwatch.GetElapsedTime(started) >= Bound * 2)
                throw new TimeoutException($"The follower compacted {compactions} times while writes went on; last retention {retention}.");
        }

        await AwaitCompactedAsync(follower, Threshold * 2, cancellationToken);
        var status = await follower.GetStatusAsync(cancellationToken);
        var retained = await follower.GetRetentionAsync(cancellationToken);

        _ = await Assert.That(File.Exists(GroupStoragePaths.GetSnapshotPath(cluster[FollowerId].DataDir, OwnerId))).IsTrue();
        _ = await Assert.That(retained.SnapshotIndex > 0 && retained.SnapshotIndex <= status.LastAppliedIndex).IsTrue()
                        .Because($"The follower compacts only through its applied index; retention {retained}, status {status}.");
        _ = await Assert.That(retained.RetainedEntries <= Threshold * 2).IsTrue().Because($"The follower group log must stay bounded; retention {retained}.");
    }

    /// <summary>
    /// A follower compacts through the applied index it persisted while its commit index is higher, and a restart recovers the snapshot,
    /// the committed entries above it, and the memory they produce; the owner then keeps appending to it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerRecoversCompactedCommittedTail(CancellationToken cancellationToken)
    {
        const string scope = "follower-log-recovery";
        const int writes = Threshold * 2;
        const int laterWrites = 4;
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true, ManualMaintenanceInterval), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        await WriteAsync(owner, key, 1, writes, cancellationToken);
        var applied = await FlushAppliedAsync(owner, cluster[FollowerId], cancellationToken);

        // Later writes raise the follower's commit index above the applied index it persisted; no pass persists a newer one.
        await WriteAsync(owner, key, writes + 1, laterWrites, cancellationToken);
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [(FollowerId, cluster[FollowerId])], cancellationToken);
        var log = GroupLog(cluster[FollowerId]);
        var policy = ReplicaLogCompactionPolicy.From(cluster[FollowerId].GetRequiredService<PersistenceOptions>());
        var outcome = await ReplicaLogCompactionStep.RunFollowerAsync(log, policy, cancellationToken);
        var compacted = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);
        _ = await Assert.That((await log.GetRetentionAsync(cancellationToken)).SnapshotIndex).IsEqualTo(applied);
        _ = await Assert.That(compacted.CommitIndex > applied).IsTrue().Because($"The commit index must run ahead of the compacted applied index {applied}; status {compacted}.");

        await cluster.StopNodeAsync(FollowerId);
        var restarted = await cluster.StartNodeAsync(FollowerId, Options(scope, false, ManualMaintenanceInterval), cancellationToken);
        var reopened = GroupLog(restarted);
        var recovered = await reopened.GetStatusAsync(cancellationToken);
        var retention = await reopened.GetRetentionAsync(cancellationToken);
        var tail = await reopened.GetCommittedEntriesAsync(applied, writes, cancellationToken);
        _ = await Assert.That((retention.SnapshotIndex, recovered.LastAppliedIndex)).IsEqualTo((applied, applied));
        _ = await Assert.That(recovered.CommitIndex >= compacted.CommitIndex).IsTrue().Because($"The restart must keep the durable commit index; status {recovered}.");
        _ = await Assert.That(tail.Count >= int.CreateChecked(compacted.CommitIndex - applied) && tail[0].LogIndex == applied + 1).IsTrue()
                        .Because("Every committed entry above the snapshot must survive the restart.");

        // The owner keeps appending right after the compacted follower's log; once the follower applies the writes, its memory holds them.
        await WriteAsync(owner, key, writes + laterWrites + 1, 1, cancellationToken);
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [(FollowerId, restarted)], cancellationToken);
        await AwaitAppliedAsync(restarted, (await GroupLog(owner).GetStatusAsync(cancellationToken)).LastLogIndex - 1, cancellationToken);
        var value = (await LocalChain(restarted).GetEntryAsync(CacheName, key, cancellationToken))?.Value as string;
        var expected = string.Equals(value, Value(writes + laterWrites), StringComparison.Ordinal) ||
                       string.Equals(value, Value(writes + laterWrites + 1), StringComparison.Ordinal);
        _ = await Assert.That(expected).IsTrue().Because($"The restarted follower must apply the committed entries above its snapshot; value {value}.");
    }

    private static NodeCacheEntry<object?> Entry(int version) => new() { Value = Value(version), Version = version };

    private static string Value(int version) => $"value-{version}";

    private static IntegrationStartOptions Options(string scope, bool clean, TimeSpan maintenanceInterval) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        PersistenceOptions = new PersistenceOptions { JournalMaxSegmentMb = 64, ReplicaLogCompactionEntries = Threshold },
        ServicesConfigure = services => SetMaintenanceInterval(services, maintenanceInterval),
    };

    /// <summary>Resolves the node's copy of the owned group log: the owner's own log, or a follower's replica of it.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The group log of the owner's group on the node.</returns>
    /// <exception cref="InvalidOperationException">The node does not serve the owner's group.</exception>
    private static IFollowerLog GroupLog(ITestNodeHost host) => host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log
        : throw new InvalidOperationException($"The node does not serve group {OwnerId}.");

    /// <summary>Resolves the node's local cache chain below replication, which the group appliers write to.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The local cache chain.</returns>
    private static ILogicalNamespacedCache<object?> LocalChain(ITestNodeHost host) =>
        host.Services.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey);

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

    /// <summary>Overwrites one key owned by node-a; write number i stores value-i at version i.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="key">The key owned by node-a.</param>
    /// <param name="from">The number of the first write.</param>
    /// <param name="count">The number of writes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task WriteAsync(ITestNodeHost owner, string key, int from, int count, CancellationToken cancellationToken)
    {
        var cache = owner.GetCache<object?>(CacheName);
        for (var i = from; i < from + count; i++)
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, Entry(i), cancellationToken);
    }

    /// <summary>Waits until the follower holds and applied the committed writes, then persists its applied index as one maintenance pass does.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="follower">The follower node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The applied index the follower persisted.</returns>
    private static async Task<ulong> FlushAppliedAsync(ITestNodeHost owner, ITestNodeHost follower, CancellationToken cancellationToken)
    {
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [(FollowerId, follower)], cancellationToken);
        var log = GroupLog(follower);
        await AwaitAppliedAsync(follower, (await log.GetStatusAsync(cancellationToken)).CommitIndex, cancellationToken);
        await follower.GetRequiredService<ReplicaGroupAppliers>().For(OwnerId).FlushAsync(log, follower.GetRequiredService<IJournalCoordinator>(), cancellationToken);
        return (await log.GetStatusAsync(cancellationToken)).LastAppliedIndex;
    }

    /// <summary>Polls the follower applier of the owner's group until it applied through <paramref name="index" />.</summary>
    /// <param name="follower">The follower node.</param>
    /// <param name="index">The log index to reach.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The follower did not apply through the index within the bound.</exception>
    private static async Task AwaitAppliedAsync(ITestNodeHost follower, ulong index, CancellationToken cancellationToken)
    {
        var applier = follower.GetRequiredService<ReplicaGroupAppliers>().For(OwnerId);
        var started = Stopwatch.GetTimestamp();
        while (applier.AppliedIndex < index)
        {
            if (Stopwatch.GetElapsedTime(started) >= Bound)
            {
                var status = await GroupLog(follower).GetStatusAsync(cancellationToken);
                throw new TimeoutException($"The follower applied through {applier.AppliedIndex}, not {index}; status {status}.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>Polls a group log until a snapshot is published and at most <paramref name="retained" /> entries remain.</summary>
    /// <param name="log">The group log.</param>
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
                throw new TimeoutException($"The follower group log was not compacted within {Bound}; last retention {retention}.");

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
            retention = await log.GetRetentionAsync(cancellationToken);
        }
    }
}
