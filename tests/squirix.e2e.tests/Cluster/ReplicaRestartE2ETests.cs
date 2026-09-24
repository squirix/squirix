using System;
using System.Threading;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>End-to-end write availability of durable RF&gt;1 groups after the owning node restarts.</summary>
public sealed class ReplicaRestartE2ETests : EndToEndTestBase
{
    /// <summary>A restarted group owner with durable RF=3 data commits new writes again without being wiped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderRestartRegainsWriteQuorum(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(LeaderRestartRegainsWriteQuorum), options, true, cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("restart-quorum", "nodeA", "restart-quorum");
        var before = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var beforeCache = await before.GetCacheAsync<string>("restart-quorum", cancellationToken);
        await beforeCache.SetAsync(key, "before-restart", cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        // The commit budget is five seconds and a failed commit never recovers on a stuck group, so a
        // generous bound only distinguishes a healthy restart from a permanently stalled group.
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var after = await cluster.ConnectClientAsync("nodeA", linked.Token);
        var afterCache = await after.GetCacheAsync<string>("restart-quorum", linked.Token);
        _ = await Assert.That((await afterCache.GetValueAsync(key, linked.Token)).Value).IsEqualTo("before-restart");

        await afterCache.SetAsync(key, "after-restart", cancellationToken: linked.Token);

        _ = await Assert.That((await afterCache.GetValueAsync(key, linked.Token)).Value).IsEqualTo("after-restart");
    }

    /// <summary>A restarted group owner commits on the remaining majority while one follower is down.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartWithFollowerDownCommits(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RestartWithFollowerDownCommits), options, true, cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("restart-minority", "nodeA", "restart-minority");
        var before = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var beforeCache = await before.GetCacheAsync<string>("restart-minority", cancellationToken);
        await beforeCache.SetAsync(key, "before-restart", cancellationToken: cancellationToken);

        await cluster.StopNodeAsync("nodeC");
        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var after = await cluster.ConnectClientAsync("nodeA", linked.Token);
        var afterCache = await after.GetCacheAsync<string>("restart-minority", linked.Token);

        await afterCache.SetAsync(key, "after-restart", cancellationToken: linked.Token);

        _ = await Assert.That((await afterCache.GetValueAsync(key, linked.Token)).Value).IsEqualTo("after-restart");
    }
}
