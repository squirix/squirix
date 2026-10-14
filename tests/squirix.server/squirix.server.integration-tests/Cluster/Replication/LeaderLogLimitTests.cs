using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// The group log of an RF=3 owner is held for a follower that is down only up to its retention limit: past it the owner compacts without that
/// follower, which is repaired by a state transfer when it returns.
/// </summary>
public sealed class LeaderLogLimitTests : NodeIntegrationTestBase
{
    private const string CacheName = "leader-log-limit";
    private const string OwnerId = "node-a";

    /// <summary>The number of writes of a large value; together they put more than the one-megabyte limit in the owner log.</summary>
    private const int Writes = 24;

    /// <summary>Bounds the wait for the owner to admit the returned follower; it only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    /// <summary>The maintenance interval of the nodes; no timer pass fires within the test, which compacts by hand.</summary>
    private static readonly TimeSpan ManualMaintenanceInterval = TimeSpan.FromDays(1);

    /// <summary>The smallest thresholds the options accept: the log is compacted, and past its retention limit, at one megabyte.</summary>
    private static readonly PersistenceOptions Persistence = new()
    {
        JournalMaxSegmentMb = 64,
        ReplicaLogCompactionMb = 1,
        ReplicaLogRetentionMaxMb = 1,
        ReplicaLogCompactionEntries = int.MaxValue,
    };

    /// <summary>
    /// A follower stopped while the owner writes past the retention limit no longer holds the owner log: the owner compacts without it, and the
    /// follower, back after that, is repaired by a state transfer and holds the last value.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastLimitFollowerIsRepaired(CancellationToken cancellationToken)
    {
        const string scope = "leader-log-limit";
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        var owner = cluster[OwnerId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        await cluster.StopNodeAsync("node-c");
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        var cache = owner.GetCache<object?>(CacheName);
        var large = new string('x', 64 * 1024);
        for (var i = 1; i <= Writes; i++)
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = $"{i}:{large}", Version = i }, cancellationToken);

        var outcome = await ReplicaNodeCommitters.OwnCommitter(owner).CompactOwnedLogAsync(
            ReplicaLogCompactionPolicy.From(Persistence),
            owner.GetRequiredService<IJournalCoordinator>(),
            cancellationToken);
        var restarted = await cluster.StartNodeAsync("node-c", Options(scope, false), cancellationToken);

        // The owner counts the transfer before it admits the follower, so the counter is read once the follower counts again.
        await (Owner: owner, Slot: SlotOf(owner, "node-c")).WaitUntilValueAsync(
            static (state, _) => ValueTask.FromResult(
                state.Owner.GetRequiredService<ReplicaGroupRegistry>().EligibilityFor(OwnerId).StateFor(state.Slot) == ReplicaParticipantState.Ready),
            Bound,
            TimeProvider.System,
            cancellationToken);
        var held = await restarted.GetRequiredService<PhysicalCache<object?>>().GetValueAsync(new CacheKey(CacheName, key), cancellationToken);
        var metrics = await HttpClient.GetStringAsync(new Uri(owner.Uri, "/metrics"), cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.CompactedPastFollower);
        _ = await Assert.That(held.Value).IsEqualTo($"{Writes}:{large}");
        _ = await Assert.That(metrics).Contains("outcome=\"transferred\"", StringComparison.Ordinal);
    }

    private static int SlotOf(ITestNodeHost host, string nodeId)
    {
        var locator = host.GetRequiredService<IReplicaGroupLocator>();
        var members = new string[locator.ReplicaCount];
        locator.GetReplicaGroup(OwnerId, members);
        return Array.IndexOf(members, nodeId);
    }

    private static IntegrationStartOptions Options(string scope, bool clean) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        PersistenceOptions = Persistence,
        ServicesConfigure = SetManualMaintenance,
    };

    /// <summary>Replaces the group log maintenance schedule so no timer pass races the test.</summary>
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
}
