using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for bootstrap endpoint transport failover with multiple live nodes.</summary>
[Immutable]
public sealed class BootstrapFailoverTests : EndToEndTestBase
{
    /// <summary>Verifies an existing client session fails over to a second live bootstrap URL when the active peer stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ContinuesOnAlternateEndpointAfterLoss(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(ContinuesOnAlternateEndpointAfterLoss), cancellationToken: cancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "bootstrap-failover");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, cancellationToken);

        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("before-loss");

        await cluster.StopNodeAsync("nodeA");

        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("before-loss");
        await cache.SetAsync(key, "after-loss", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("after-loss");
    }
}
