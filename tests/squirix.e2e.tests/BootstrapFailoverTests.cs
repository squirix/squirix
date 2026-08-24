using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for bootstrap endpoint transport failover with multiple live nodes.</summary>
[Immutable]
public sealed class BootstrapFailoverTests : EndToEndTestBase
{
    /// <summary>Verifies an existing client session fails over to a second live bootstrap URL when the active peer stops.</summary>
    [Fact]
    public async Task ContinuesOnAlternateEndpointAfterLoss()
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(ContinuesOnAlternateEndpointAfterLoss), cancellationToken: DefaultCancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "bootstrap-failover");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: DefaultCancellationToken);
        Assert.Equal("before-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);

        await cluster.StopNodeAsync("nodeA");

        Assert.Equal("before-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
        await cache.SetAsync(key, "after-loss", cancellationToken: DefaultCancellationToken);
        Assert.Equal("after-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
    }
}
