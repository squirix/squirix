using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// End-to-end coverage for multi-endpoint client bootstrap connect semantics.
/// </summary>
public sealed class ClientBootstrapConnectTests : E2ETestBase
{
    /// <summary>
    /// Verifies client connect succeeds when only one configured bootstrap endpoint is reachable.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientConnectsWhenAnyBootstrapEndpointIsReachable()
    {
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientConnectsWhenAnyBootstrapEndpointIsReachable), cancellationToken: DefaultCancellationToken);
        var liveUrl = cluster.GetAddress("nodeA");

        await using var client = await E2ETestConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(liveUrl);
                options.Endpoints.Add("https://127.0.0.1:1");
            },
            DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }
}
