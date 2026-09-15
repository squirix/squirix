using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for bootstrap failover under a single absolute deadline.</summary>
public sealed class FailoverDeadlineE2ETests : EndToEndTestBase
{
    /// <summary>Failover to the surviving endpoint completes within one absolute deadline after peer loss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailoverRespectsOneAbsoluteDeadline(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(FailoverRespectsOneAbsoluteDeadline), cancellationToken: cancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "failover-deadline");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, cancellationToken);

        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: cancellationToken);

        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var startedUtc = DateTime.UtcNow;

        await cache.SetAsync(key, "after-loss", cancellationToken: linked.Token);
        _ = await Assert.That((await cache.GetValueAsync(key, linked.Token)).Value).IsEqualTo("after-loss");
        _ = await Assert.That(DateTime.UtcNow - startedUtc < TimeSpan.FromSeconds(20)).IsTrue();
    }
}
