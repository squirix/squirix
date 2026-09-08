using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Replication;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for offline bootstrap seeding of a stopped node.</summary>
public sealed class ReplicaBootstrapE2ETests : EndToEndTestBase
{
    /// <summary>Verifies offline bootstrap prepares replica groups for a stopped node without touching its data.</summary>
    [Fact]
    public Task OfflineBootstrapSeedsStoppedNodeGroups() => SeedAndVerifyAsync(nameof(OfflineBootstrapSeedsStoppedNodeGroups), "bootstrap-seed", 3, 2UL, DefaultCancellationToken);

    /// <summary>Offline RF=1 bootstrap seeds replica groups for a stopped node without touching its data.</summary>
    /// <remarks>
    /// RF=1 denotes the offline source node (single-node RF=1); the bootstrap target is RF&gt;1 by definition
    /// (the planner rejects target replica counts of one or less), so this scenario seeds toward RF=2.
    /// </remarks>
    [Fact]
    public Task OfflineRfOneBootstrapSeedsReplicaGroups() => SeedAndVerifyAsync(
        nameof(OfflineRfOneBootstrapSeedsReplicaGroups),
        "bootstrap-rf-one",
        2,
        3UL,
        DefaultCancellationToken);

    private static async Task SeedAndVerifyAsync(string nodeName, string cacheName, int targetReplicaCount, ulong targetGeneration, CancellationToken cancellationToken)
    {
        await using var node = await RestartableNode.StartAsync(nodeName, cancellationToken);
        var cache = await node.GetCacheAsync<string>(cacheName, cancellationToken);
        await cache.SetAsync("seeded", "value", cancellationToken: cancellationToken);
        await node.StopAsync();

        var summary = await OfflineBootstrapTestKit.PrepareAsync(node.DataDir, ["group-a", "group-b"], targetReplicaCount, targetGeneration, cancellationToken);

        Assert.Equal(targetReplicaCount, summary.TargetReplicaCount);
        Assert.Equal(targetGeneration, summary.TargetGeneration);
        Assert.Equal(["group-a:Pending", "group-b:Pending"], summary.PendingGroups);
        Assert.False(summary.Resumed);

        await node.RestartAsync(cancellationToken);
        var restarted = await node.GetCacheAsync<string>(cacheName, cancellationToken);
        var result = await restarted.GetValueAsync("seeded", cancellationToken);

        Assert.True(result.Found, "Seeded entry was not visible after the restart.");
        Assert.Equal("value", result.Value);
    }
}
