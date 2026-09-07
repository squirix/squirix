using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for bootstrap failover under a single absolute deadline.</summary>
public sealed class FailoverDeadlineE2ETests : EndToEndTestBase
{
    /// <summary>Failover to the surviving endpoint completes within one absolute deadline after peer loss.</summary>
    [Fact]
    public async Task FailoverRespectsOneAbsoluteDeadline()
    {
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(FailoverRespectsOneAbsoluteDeadline), cancellationToken: DefaultCancellationToken);
        var uriA = cluster.GetUri("nodeA");
        var uriB = cluster.GetUri("nodeB");
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "failover-deadline");

        await using var client = await LoopbackConnect.ConnectAsync(uriA, uriB, DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync(key, "before-loss", cancellationToken: DefaultCancellationToken);

        await cluster.StopNodeAsync("nodeA");

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(DefaultCancellationToken, deadline.Token);
        var startedUtc = DateTime.UtcNow;

        await cache.SetAsync(key, "after-loss", cancellationToken: linked.Token);
        Assert.Equal("after-loss", (await cache.GetValueAsync(key, linked.Token)).Value);
        Assert.True(DateTime.UtcNow - startedUtc < TimeSpan.FromSeconds(20));
    }
}
