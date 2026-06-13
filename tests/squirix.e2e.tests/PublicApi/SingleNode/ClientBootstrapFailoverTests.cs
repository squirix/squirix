using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// End-to-end coverage for bootstrap endpoint transport failover with multiple live nodes.
/// </summary>
public sealed class ClientBootstrapFailoverTests : E2ETestBase
{
    /// <summary>
    /// Verifies an existing client session fails over to a second live bootstrap URL when the active peer stops.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientContinuesOnAlternateBootstrapAfterActiveEndpointLoss()
    {
        await using var cluster = await E2ECluster.StartTwoNodeAsync(
            nameof(ClientContinuesOnAlternateBootstrapAfterActiveEndpointLoss),
            cancellationToken: DefaultCancellationToken);
        var urlA = cluster.GetAddress("nodeA");
        var urlB = cluster.GetAddress("nodeB");
        var key = new E2EKeyOwnerHelper(["nodeA", "nodeB"]).FindKeysOwnedBy("default", "nodeB", 1, "bootstrap-failover")[0];

        await using var client = await E2ETestConnect.ConnectAsync(
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
