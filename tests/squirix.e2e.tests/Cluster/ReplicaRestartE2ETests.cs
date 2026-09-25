using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.TestKit;
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

    /// <summary>
    /// A group owner killed after a write reached only its own durable log recovers that write once it restarts next to followers that
    /// never received it: the write is committed and applied before any new one, and the group accepts writes again without a wipe.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedTailRestartRegainsQuorum(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(UncommittedTailRestartRegainsQuorum), options, true, cancellationToken);
        var tailKey = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("restart-tail", "nodeA", "restart-tail");
        var newKey = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("restart-tail", "nodeA", "restart-new");
        var before = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var beforeCache = await before.GetCacheAsync<string>("restart-tail", cancellationToken);
        await beforeCache.SetAsync(tailKey, "committed", cancellationToken: cancellationToken);

        // Without its followers the owner appends the write durably but cannot commit it: the outcome is unknown.
        await cluster.StopNodeAsync("nodeB");
        await cluster.StopNodeAsync("nodeC");
        _ = await NodeAsyncAssert.ThrowsAsync<CommitOutcomeUnknownException>(beforeCache.SetAsync(tailKey, "tail", cancellationToken: cancellationToken));
        await cluster.GetNode("nodeA").AbruptShutdownAsync();
        await cluster.RestartNodeAsync("nodeB", cancellationToken);
        await cluster.RestartNodeAsync("nodeC", cancellationToken);
        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        // Writes are refused until the owner's readiness verification re-sends the tail and commits it, so the first write is retried
        // within a bound that only distinguishes a recovering group from a permanently stalled one.
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var after = await cluster.ConnectClientAsync("nodeA", linked.Token);
        var afterCache = await after.GetCacheAsync<string>("restart-tail", linked.Token);
        await SetWhenWritableAsync(afterCache, newKey, "after-restart", linked.Token);

        _ = await Assert.That((await afterCache.GetValueAsync(tailKey, linked.Token)).Value).IsEqualTo("tail");
        _ = await Assert.That((await afterCache.GetValueAsync(newKey, linked.Token)).Value).IsEqualTo("after-restart");
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

    private static async Task SetWhenWritableAsync(ICache<string> cache, string key, string value, CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await cache.SetAsync(key, value, cancellationToken: cancellationToken);
                return;
            }
            catch (RpcException)
            {
                // Refused before the append while the recovered tail is still pending: nothing was written, so retrying is safe.
                await Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, cancellationToken);
            }
        }
    }
}
