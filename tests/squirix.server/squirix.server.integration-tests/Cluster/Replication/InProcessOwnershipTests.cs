using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>In-process callers of the cache pipeline are refused for keys another node owns, before anything commits or applies.</summary>
public sealed class InProcessOwnershipTests : NodeIntegrationTestBase
{
    private const string CacheName = "in-process-ownership";

    /// <summary>Bounds the verification of a fresh cluster.</summary>
    private static readonly TimeSpan VerificationBound = TimeSpan.FromSeconds(30);

    /// <summary>A write through the entry pipeline for a key another node owns is refused and leaves the replica group log untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteKeyWriteLeavesGroupLogUntouched(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("in-process-ownership-rf3"), cancellationToken);
        var node = cluster["node-a"];
        await WaitForReplicasAsync(node, cancellationToken);
        _ = node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog("node-a", out var log);
        var before = await log!.GetStatusAsync(cancellationToken);
        var key = node.FindKeyOwnedBy(CacheName, "node-b");
        var pipeline = node.GetRequiredService<ILogicalNamespacedCache<object?>>();

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(pipeline.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "v", Version = 1 }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var after = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex);
        _ = await Assert.That(after.LastAppliedIndex).IsEqualTo(before.LastAppliedIndex);
    }

    /// <summary>A read and a write through the entry pipeline of a single-copy host are refused for a remote key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteKeyIsRefusedOnSingleCopyHost(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", cancellationToken: cancellationToken);
        var node = cluster["node-a"];
        var key = node.FindKeyOwnedBy(CacheName, "node-b");
        var pipeline = node.GetRequiredService<ILogicalNamespacedCache<object?>>();

        var read = await NodeAsyncAssert.ThrowsAsync<ServerCallException, NodeCacheValueResult<object?>>(pipeline.GetValueAsync(CacheName, key, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var write = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(pipeline.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "v", Version = 1 }, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(read.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(read.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(write.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(write.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
    }

    private static IntegrationStartOptions Options(string scope) => new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = scope };

    private static async Task WaitForReplicasAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(VerificationBound);
        var committer = ReplicaNodeCommitters.OwnCommitter(node);
        while (await committer.VerifyReplicasAsync(deadline.Token) != ReplicaVerification.AllReady)
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, deadline.Token);
    }
}
