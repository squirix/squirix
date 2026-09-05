using System.Threading.Tasks;
using Squirix.Server.TestKit.Replication;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for offline bootstrap seeding of a stopped node.</summary>
public sealed class ReplicaBootstrapE2ETests : EndToEndTestBase
{
    /// <summary>Verifies offline bootstrap prepares replica groups for a stopped node without touching its data.</summary>
    [Fact]
    public async Task OfflineBootstrapSeedsStoppedNodeGroups()
    {
        await using var node = await RestartableNode.StartAsync(nameof(OfflineBootstrapSeedsStoppedNodeGroups), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("bootstrap-seed", DefaultCancellationToken);
        await cache.SetAsync("seeded", "value", cancellationToken: DefaultCancellationToken);
        await node.StopAsync();

        var summary = await OfflineBootstrapTestKit.PrepareAsync(node.DataDir, ["group-a", "group-b"], DefaultCancellationToken);

        Assert.Equal(3, summary.TargetReplicaCount);
        Assert.Equal(2UL, summary.TargetGeneration);
        Assert.Equal(["group-a:Pending", "group-b:Pending"], summary.PendingGroups);
        Assert.False(summary.Resumed);

        await node.RestartAsync(DefaultCancellationToken);
        var restarted = await node.GetCacheAsync<string>("bootstrap-seed", DefaultCancellationToken);
        var result = await restarted.GetValueAsync("seeded", DefaultCancellationToken);

        Assert.True(result.Found, "Seeded entry was not visible after the restart.");
        Assert.Equal("value", result.Value);
    }
}
