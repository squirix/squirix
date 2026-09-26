using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for offline bootstrap seeding of a stopped node.</summary>
public sealed class ReplicaBootstrapE2ETests : EndToEndTestBase
{
    /// <summary>Bootstrap targets beyond the fixture peers are rejected at the call site.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public void BootstrapTargetBeyondPeersIsRejected(CancellationToken cancellationToken)
    {
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            (Dir: "bootstrap-invalid", TargetReplicaCount: 4, Token: cancellationToken),
            static state => _ = OfflineBootstrapTestKit.PrepareAsync(state.Dir, ["group-a"], state.TargetReplicaCount, 2UL, state.Token));
    }

    /// <summary>Verifies offline bootstrap prepares replica groups for a stopped node without touching its data.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task OfflineBootstrapSeedsStoppedNodeGroups(CancellationToken cancellationToken) => SeedAndVerifyAsync(
        nameof(OfflineBootstrapSeedsStoppedNodeGroups),
        "bootstrap-seed",
        3,
        2UL,
        cancellationToken);

    /// <summary>Offline RF=1 bootstrap seeds replica groups for a stopped node without touching its data.</summary>
    /// <remarks>
    /// RF=1 denotes the offline source node (single-node RF=1); the bootstrap target is RF&gt;1 by definition
    /// (the planner rejects target replica counts of one or less), so this scenario seeds toward RF=2.
    /// </remarks>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task OfflineRfOneBootstrapSeedsReplicaGroups(CancellationToken cancellationToken) => SeedAndVerifyAsync(
        nameof(OfflineRfOneBootstrapSeedsReplicaGroups),
        "bootstrap-rf-one",
        2,
        3UL,
        cancellationToken);

    private static async Task SeedAndVerifyAsync(string nodeName, string cacheName, int targetReplicaCount, ulong targetGeneration, CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nodeName,
            persistence: true,
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>(cacheName, cancellationToken: cancellationToken);
        await cache.SetAsync("seeded", "value", cancellationToken: cancellationToken);
        await cluster.StopNodeAsync("nodeA");

        var summary = await OfflineBootstrapTestKit.PrepareAsync(cluster.GetDataDir("nodeA"), ["group-a", "group-b"], targetReplicaCount, targetGeneration, cancellationToken);

        _ = await Assert.That(summary.TargetReplicaCount).IsEqualTo(targetReplicaCount);
        _ = await Assert.That(summary.TargetGeneration).IsEqualTo(targetGeneration);
        await SequenceAssert.EqualAsync(["group-a:Pending", "group-b:Pending"], summary.PendingGroups, StringComparer.Ordinal);
        _ = await Assert.That(summary.Resumed).IsFalse();

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restarted = await cluster.GetCacheAsync<string>(cacheName, cancellationToken: cancellationToken);
        var result = await restarted.GetValueAsync("seeded", cancellationToken);

        _ = await Assert.That(result.Found).IsTrue().Because("Seeded entry was not visible after the restart.");
        _ = await Assert.That(result.Value).IsEqualTo("value");
    }
}
