using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for replica repair through node restart and recovery.</summary>
public sealed class ReplicaRepairE2ETests : EndToEndTestBase
{
    /// <summary>Verifies a restarted node completes recovery and serves committed entries.</summary>
    [Fact]
    public async Task RestartedNodeServesCommitted()
    {
        await using var node = await RestartableNode.StartAsync(nameof(RestartedNodeServesCommitted), DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("repair-recovery", DefaultCancellationToken);
        await cache.SetAsync("one", "1", cancellationToken: DefaultCancellationToken);
        await cache.SetAsync("two", "2", cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);
        var restarted = await node.GetCacheAsync<string>("repair-recovery", DefaultCancellationToken);

        var one = await restarted.GetValueAsync("one", DefaultCancellationToken);
        var two = await restarted.GetValueAsync("two", DefaultCancellationToken);
        Assert.True(one.Found, "Committed entry 'one' was not served after recovery.");
        Assert.Equal("1", one.Value);
        Assert.True(two.Found, "Committed entry 'two' was not served after recovery.");
        Assert.Equal("2", two.Value);
    }
}
