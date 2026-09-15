using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for multi-endpoint client bootstrap connect semantics.</summary>
[Immutable]
public sealed class BootstrapConnectTests : EndToEndTestBase
{
    /// <summary>Verifies public client connect succeeds when only one configured bootstrap endpoint is reachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectsViaAnyReachableBootstrapEndpoint(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(ConnectsViaAnyReachableBootstrapEndpoint),
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(uri, new Uri("https://127.0.0.1:1"), cancellationToken);

        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Value).IsEqualTo("v");
    }
}
