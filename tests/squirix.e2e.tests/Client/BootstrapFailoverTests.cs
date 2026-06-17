using System.Threading.Tasks;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Client;
using Squirix.E2ETests.Support.Cluster;
using Xunit;

namespace Squirix.E2ETests.Client;

/// <summary>End-to-end coverage for bootstrap endpoint transport failover with multiple live nodes.</summary>
public sealed class BootstrapFailoverTests : EndToEndTestBase
{
    /// <summary>Verifies an existing client session fails over to a second live bootstrap URL when the active peer stops.</summary>
    [Fact]
    public async Task ClientContinuesOnAlternateBootstrapAfterActiveEndpointLoss()
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(
            nameof(ClientContinuesOnAlternateBootstrapAfterActiveEndpointLoss),
            cancellationToken: DefaultCancellationToken);
        var urlA = cluster.GetAddress("nodeA");
        var urlB = cluster.GetAddress("nodeB");
        var key = new KeyOwnerHelper(["nodeA", "nodeB"]).FindKeysOwnedBy("default", "nodeB", 1, "bootstrap-failover")[0];

        await using var client = await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(urlA);
                options.Endpoints.Add(urlB);
            },
            DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: DefaultCancellationToken);
        Assert.Equal("before-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);

        await cluster.StopNodeAsync("nodeA");

        Assert.Equal("before-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
        await cache.SetAsync(key, "after-loss", cancellationToken: DefaultCancellationToken);
        Assert.Equal("after-loss", (await cache.GetValueAsync(key, DefaultCancellationToken)).Value);
    }
}
