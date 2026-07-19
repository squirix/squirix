using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for multi-endpoint client bootstrap connect semantics.</summary>
public sealed class BootstrapConnectTests : EndToEndTestBase
{
    /// <summary>Verifies public client connect succeeds when only one configured bootstrap endpoint is reachable.</summary>
    [Fact]
    public async Task ClientConnectsWhenAnyBootstrapEndpointIsReachable()
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ClientConnectsWhenAnyBootstrapEndpointIsReachable), cancellationToken: DefaultCancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(uri);
                options.Endpoints.Add(new Uri("https://127.0.0.1:1"));
            },
            DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: DefaultCancellationToken);
        Assert.Equal("v", (await cache.GetValueAsync("k", DefaultCancellationToken)).Value);
    }
}
