using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>An RF=3 group owner advances its applied index with its commit index and releases the applied payloads.</summary>
public sealed class GroupLogRetentionTests : NodeIntegrationTestBase
{
    private const string CacheName = "group-log-retention";
    private const string OwnerId = "node-a";
    private const int Overwrites = 20;

    /// <summary>Bounds the wait for the applied index to reach the commit index; a healthy owner applies within a few rounds.</summary>
    private static readonly TimeSpan ApplyBound = TimeSpan.FromSeconds(10);

    /// <summary>The group log maintenance interval of the test nodes, far below the production default so the bound above holds.</summary>
    private static readonly TimeSpan MaintenanceInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>The owner's applied index follows its commit index, so no committed payload stays retained in memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderAppliedFollowsCommit(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("group-log-applied", true), cancellationToken);
        var owner = cluster[OwnerId];
        await OverwriteAsync(owner, cancellationToken);

        var log = OwnerLog(owner);
        var status = await AwaitAppliedAsync(log, cancellationToken);
        var retained = await log.GetCommittedEntriesAsync(cancellationToken);

        _ = await Assert.That(status.CommitIndex >= Overwrites).IsTrue().Because("Every overwrite must be committed on the owner's group log.");
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(status.CommitIndex).Because("The owner's applied index must follow its commit index.");
        _ = await Assert.That(retained.Count).IsEqualTo(0).Because("Applied payloads must be released, so no committed entry stays retained.");
    }

    /// <summary>A restarted owner persists its applied index and reloads no payload that was already applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderRestartLoadsOnlyUnappliedPayloads(CancellationToken cancellationToken)
    {
        const string scope = "group-log-restart";
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        await OverwriteAsync(cluster[OwnerId], cancellationToken);
        _ = await AwaitAppliedAsync(OwnerLog(cluster[OwnerId]), cancellationToken);

        await cluster.StopNodeAsync(OwnerId);
        var restarted = await cluster.StartNodeAsync(OwnerId, Options(scope, false), cancellationToken);
        var log = OwnerLog(restarted);
        var retained = await log.GetCommittedEntriesAsync(cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.CommitIndex >= Overwrites).IsTrue().Because("Every overwrite must stay committed across the restart.");
        _ = await Assert.That(retained.Count).IsEqualTo(0).Because("A restarted owner must not reload payloads it had already applied.");
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(status.CommitIndex).Because("The owner's applied index must persist across the restart.");
    }

    private static IntegrationStartOptions Options(string scope, bool clean) =>
        new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = clean, ExtraScope = scope, ServicesConfigure = ShortenMaintenance };

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

    private static IFollowerLog OwnerLog(ITestNodeHost owner)
    {
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log);
        return log!;
    }

    /// <summary>Overwrites one key owned by node-a, each write with a fresh operation identifier.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task OverwriteAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        for (var i = 1; i <= Overwrites; i++)
        {
            var entry = new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i };
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, entry, cancellationToken);
        }
    }

    /// <summary>Polls the owner's log status until its applied index reaches its commit index or the bound elapses.</summary>
    /// <param name="log">The owner's group log.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last observed status; the caller asserts on it.</returns>
    private static async Task<FollowerLogStatus> AwaitAppliedAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var status = await log.GetStatusAsync(cancellationToken);
        while (status.LastAppliedIndex != status.CommitIndex && Stopwatch.GetElapsedTime(started) < ApplyBound)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, cancellationToken);
            status = await log.GetStatusAsync(cancellationToken);
        }

        return status;
    }
}
