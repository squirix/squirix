using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests.Cluster;

/// <summary>End-to-end synchronous replication over a three-node RF=3 cluster.</summary>
public sealed class SynchronousReplicationE2ETests : EndToEndTestBase
{
    /// <summary>A canceled writing commits nothing; the retry commits exactly one effect.</summary>
    [Fact(DisplayName = "SynchronousReplicationE2ETests.CancelledWaitRetryProducesOneCommittedEffect")]
    public async Task CancelledWaitRetryCommitsOnce()
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(CancelledWaitRetryCommitsOnce), options, true, DefaultCancellationToken);
        var client = await cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("sync-cancel", DefaultCancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("sync-cancel", "nodeA", "cancel");

        await cache.SetAsync(key, "v0", cancellationToken: DefaultCancellationToken);

        // The wait is canceled before the commit starts: nothing may be committed.
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(cache.SetAsync(key, "v1", cancellationToken: cancelled.Token));

        var untouched = await cache.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(untouched.Found);
        Assert.Equal("v0", untouched.Value);

        // The retry commits exactly one visible effect.
        await cache.SetAsync(key, "v1", cancellationToken: DefaultCancellationToken);
        var read = await cache.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v1", read.Value);
    }

    /// <summary>RF=3 current reads are served locally without consulting a quorum.</summary>
    [Fact]
    public async Task RfThreeCurrentReadRequiresQuorumReadGate()
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RfThreeCurrentReadRequiresQuorumReadGate), options, true, DefaultCancellationToken);
        var client = await cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("quorum-read", DefaultCancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("quorum-read", "nodeA", "read");

        await cache.SetAsync(key, "v", cancellationToken: DefaultCancellationToken);

        // No majority remains, yet the current read is still served: quorum reads stay gated.
        await cluster.StopNodeAsync("nodeB");
        await cluster.StopNodeAsync("nodeC");

        var read = await cache.GetValueAsync(key, DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v", read.Value);
    }
}
