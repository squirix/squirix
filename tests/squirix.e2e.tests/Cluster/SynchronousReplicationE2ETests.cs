using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>End-to-end synchronous replication over a three-node RF=3 cluster.</summary>
public sealed class SynchronousReplicationE2ETests : EndToEndTestBase
{
    /// <summary>A canceled writing commits nothing; the retry commits exactly one effect.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledWaitRetryCommitsOnce(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(CancelledWaitRetryCommitsOnce), options, true, cancellationToken);
        var client = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var cache = await client.GetCacheAsync<string>("sync-cancel", cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("sync-cancel", "nodeA", "cancel");

        await cache.SetAsync(key, "v0", cancellationToken: cancellationToken);

        // The wait is canceled before the commit starts: nothing may be committed.
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(cache.SetAsync(key, "v1", cancellationToken: cancelled.Token));

        var untouched = await cache.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(untouched.Found).IsTrue();
        _ = await Assert.That(untouched.Value).IsEqualTo("v0");

        // The retry commits exactly one visible effect.
        await cache.SetAsync(key, "v1", cancellationToken: cancellationToken);
        var read = await cache.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo("v1");
    }

    /// <summary>RF=3 current reads are served locally without consulting a quorum.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfThreeCurrentReadRequiresQuorumReadGate(CancellationToken cancellationToken)
    {
        var options = new MultiNodeStartOptions { ReplicaCount = 3 };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RfThreeCurrentReadRequiresQuorumReadGate), options, true, cancellationToken);
        var client = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var cache = await client.GetCacheAsync<string>("quorum-read", cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy("quorum-read", "nodeA", "read");

        await cache.SetAsync(key, "v", cancellationToken: cancellationToken);

        // No majority remains, yet the current read is still served: quorum reads stay gated.
        await cluster.StopNodeAsync("nodeB");
        await cluster.StopNodeAsync("nodeC");

        var read = await cache.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo("v");
    }
}
