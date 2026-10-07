using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
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
/// The followers of an RF=3 group apply the committed entries they receive over the network to their own memory, and a restarted
/// follower applies them again from its group log.
/// </summary>
public sealed class FollowerApplyTests : NodeIntegrationTestBase
{
    private const string CacheName = "follower-apply";
    private const string OwnerId = "node-a";

    /// <summary>The number of writes; a follower learns the commit of the last one only with a later append, so only the ones before it are checked.</summary>
    private const int Writes = 4;

    /// <summary>Bounds every wait; a healthy follower applies an entry within milliseconds, so the bound only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static readonly string[] FollowerIds = ["node-b", "node-c"];

    /// <summary>The maintenance interval of the test nodes; no pass persists an applied index within a test.</summary>
    private static readonly TimeSpan ManualMaintenanceInterval = TimeSpan.FromDays(1);

    /// <summary>Each follower's memory holds every write it knows as committed, read below replication on the follower itself.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerMemoryConvergesAfterAppend(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("follower-apply-converge", true), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var (first, keys) = await WriteAsync(owner, cancellationToken);
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [("node-b", cluster["node-b"]), ("node-c", cluster["node-c"])], cancellationToken);

        foreach (var follower in FollowerIds)
        {
            var host = cluster[follower];
            await AwaitAppliedAsync(host, first + Writes - 2, cancellationToken);
            for (var i = 0; i < Writes - 1; i++)
            {
                var entry = await LocalChain(host).GetEntryAsync(CacheName, keys[i], cancellationToken);
                _ = await Assert.That(entry?.Value).IsEqualTo(Value(i + 1)).Because($"{follower} must hold the committed write {i + 1} in memory.");
            }
        }
    }

    /// <summary>A restarted follower applies the committed entries again from its group log, from the durable applied index it persisted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerRestartReappliesCommittedEntries(CancellationToken cancellationToken)
    {
        const string scope = "follower-apply-restart";
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var (first, keys) = await WriteAsync(owner, cancellationToken);
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, OwnerId, [("node-b", cluster["node-b"])], cancellationToken);
        await AwaitAppliedAsync(cluster["node-b"], first + Writes - 2, cancellationToken);

        await cluster.StopNodeAsync("node-b");
        var restarted = await cluster.StartNodeAsync("node-b", Options(scope, false), cancellationToken);
        var log = GroupLog(restarted);
        var start = await log.GetStatusAsync(cancellationToken);
        await AwaitAppliedAsync(restarted, start.CommitIndex, cancellationToken);

        // The applier starts from the durable applied index and moves only by applying an entry; flushing it persists how far it got.
        await restarted.GetRequiredService<ReplicaFollowerAppliers>().For(OwnerId).FlushAsync(log, restarted.GetRequiredService<IJournalCoordinator>(), cancellationToken);
        var flushed = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(start.CommitIndex >= first + Writes - 2).IsTrue().Because($"The restarted follower must keep its durable commit index; commit {start.CommitIndex}.");
        _ = await Assert.That(start.LastAppliedIndex).IsEqualTo(0UL).Because("No maintenance pass ran before the restart, so the restart point is the start of the log.");
        _ = await Assert.That(flushed.LastAppliedIndex).IsEqualTo(start.CommitIndex).Because("The restarted follower applied every committed entry again from the restart point.");
        for (var i = 0; i < Writes - 1; i++)
        {
            var entry = await LocalChain(restarted).GetEntryAsync(CacheName, keys[i], cancellationToken);
            _ = await Assert.That(entry?.Value).IsEqualTo(Value(i + 1));
        }
    }

    private static string Value(int write) => $"value-{write}";

    private static IntegrationStartOptions Options(string scope, bool clean) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        PersistenceOptions = new PersistenceOptions { JournalMaxSegmentMb = 64 },
        ServicesConfigure = SetManualMaintenance,
    };

    /// <summary>Replaces the group log maintenance schedule, so no pass persists an applied index within a test.</summary>
    /// <param name="services">The node service collection.</param>
    private static void SetManualMaintenance(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = ManualMaintenanceInterval });
    }

    private static IFollowerLog GroupLog(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group {OwnerId}.");

    /// <summary>Resolves the node's local cache chain below replication, which the group appliers write to.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The local cache chain.</returns>
    private static ILogicalNamespacedCache<object?> LocalChain(ITestNodeHost host) =>
        host.Services.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey);

    /// <summary>Writes distinct keys owned by node-a, one entry each.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The log index of the first write and the keys, in write order; write number i stores value-i.</returns>
    private static async Task<(ulong First, string[] Keys)> WriteAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var first = (await GroupLog(owner).GetStatusAsync(cancellationToken)).LastLogIndex + 1;
        var cache = owner.GetCache<object?>(CacheName);
        var keys = OwnedKeys(owner);
        for (var i = 0; i < Writes; i++)
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, keys[i], new NodeCacheEntry<object?> { Value = Value(i + 1), Version = i + 1 }, cancellationToken);

        return (first, keys);
    }

    /// <summary>Finds distinct keys owned by node-a, one per write.</summary>
    /// <param name="owner">The group owner.</param>
    /// <returns>The keys.</returns>
    /// <exception cref="InvalidOperationException">Too few owned keys were found.</exception>
    private static string[] OwnedKeys(ITestNodeHost owner)
    {
        var locator = owner.GetRequiredService<INodeLocator>();
        var keys = new string[Writes];
        var found = 0;
        for (var candidate = 0; candidate < 10_000 && found < Writes; candidate++)
        {
            var key = $"{CacheName}-{candidate}";
            if (string.Equals(locator.GetOwner(CacheName, key), OwnerId, StringComparison.Ordinal))
                keys[found++] = key;
        }

        return found == Writes ? keys : throw new InvalidOperationException($"Only {found} keys owned by {OwnerId} were found.");
    }

    /// <summary>Polls the follower applier of the owner's group until it applied through <paramref name="index" />.</summary>
    /// <param name="host">The follower node.</param>
    /// <param name="index">The log index to reach.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The follower did not apply through the index within the bound.</exception>
    private static async Task AwaitAppliedAsync(ITestNodeHost host, ulong index, CancellationToken cancellationToken)
    {
        var applier = host.GetRequiredService<ReplicaFollowerAppliers>().For(OwnerId);
        var started = Stopwatch.GetTimestamp();
        while (applier.AppliedIndex < index)
        {
            if (Stopwatch.GetElapsedTime(started) >= Bound)
            {
                var status = await GroupLog(host).GetStatusAsync(cancellationToken);
                throw new TimeoutException($"The follower applied through {applier.AppliedIndex}, not {index}; its log ends at {status.LastLogIndex}, commit {status.CommitIndex}.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), TimeProvider.System, cancellationToken);
        }
    }
}
