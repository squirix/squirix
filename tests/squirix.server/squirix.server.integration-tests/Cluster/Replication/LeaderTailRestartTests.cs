using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>An RF=3 group owner killed between the local append of a write and its commit recovers the tail after the restart.</summary>
public sealed class LeaderTailRestartTests : NodeIntegrationTestBase
{
    private const string CacheName = "leader-tail";
    private const string TailCacheName = "leader-tail-pending";

    /// <summary>Bounds the verification after the restart; a healthy restart verifies within a few probe rounds.</summary>
    private static readonly TimeSpan VerificationBound = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The uncommitted write is committed and applied after the owner restarts next to followers that never received it, and the
    /// group accepts new writes again without being wiped.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderTailCommitsAfterRestart(CancellationToken cancellationToken)
    {
        const string scope = "leader-tail-commit";
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        var (owner, key) = await CrashWithTailAsync(cluster, scope, Guid.NewGuid().ToString("N"), cancellationToken);
        var cache = owner.GetCache<object?>(TailCacheName);

        await VerifyAsync(owner, cancellationToken);
        var recovered = await cache.GetValueAsync(TailCacheName, key, cancellationToken);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), TailCacheName, key, new NodeCacheEntry<object?> { Value = "after", Version = 2 }, cancellationToken);

        _ = await Assert.That(recovered.Found).IsTrue();
        _ = await Assert.That(recovered.Value).IsEqualTo("tail");
        var status = await OwnerStatusAsync(owner, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((3UL, 3UL));
    }

    /// <summary>A retry of the uncommitted write after the restart replays its outcome instead of appending it a second time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartTailNotDuplicatedOnRetry(CancellationToken cancellationToken)
    {
        const string scope = "leader-tail-retry";
        var operationId = Guid.NewGuid().ToString("N");
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options(scope, true), cancellationToken);
        var (owner, key) = await CrashWithTailAsync(cluster, scope, operationId, cancellationToken);

        await VerifyAsync(owner, cancellationToken);

        // The owner's committer is driven directly: the conditional add's memory-admission layer answers an existing key before it.
        var added = await owner.GetRequiredService<ReplicaGroupCommitter>().CommitTryAddAsync(operationId, TailCacheName, key, TailEntry(), cancellationToken);

        _ = await Assert.That(added).IsTrue();
        var status = await OwnerStatusAsync(owner, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 2UL));
    }

    private static IntegrationStartOptions Options(string scope, bool clean) =>
        new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = clean, ExtraScope = scope };

    private static NodeCacheEntry<object?> TailEntry() => new() { Value = "tail", Version = 1 };

    private static async Task<FollowerLogStatus> OwnerStatusAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog("node-a", out var log);
        return await log!.GetStatusAsync(cancellationToken);
    }

    /// <summary>Verifies the restarted owner's replica slots, as its readiness service does, until every slot counts again.</summary>
    /// <param name="owner">The restarted group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task VerifyAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(VerificationBound);
        var committer = owner.GetRequiredService<ReplicaGroupCommitter>();
        while (await committer.VerifyReplicasAsync(deadline.Token) != ReplicaVerification.AllReady)
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, deadline.Token);
    }

    /// <summary>
    /// Commits one write on node-a's group, stops both followers, lets a conditional add reach only node-a's durable log (its outcome
    /// is unknown after the commit budget), kills node-a, and restarts all three nodes on their data.
    /// </summary>
    /// <param name="cluster">The running RF=3 cluster.</param>
    /// <param name="scope">Persistence scope of the cluster.</param>
    /// <param name="operationId">Operation identifier of the uncommitted conditional add.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The restarted owner and the key of the uncommitted conditional add.</returns>
    private static async Task<(ITestNodeHost Owner, string Key)> CrashWithTailAsync(
        TestCluster<IntegrationStartOptions> cluster,
        string scope,
        string operationId,
        CancellationToken cancellationToken)
    {
        var owner = cluster["node-a"];
        var committedKey = owner.FindKeyOwnedBy(CacheName, "node-a");
        var tailKey = owner.FindKeyOwnedBy(TailCacheName, "node-a");
        var committed = owner.GetCache<object?>(CacheName);
        await committed.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, committedKey, new NodeCacheEntry<object?> { Value = "committed", Version = 1 }, cancellationToken);

        await cluster.StopNodeAsync("node-b");
        await cluster.StopNodeAsync("node-c");
        var pending = owner.GetCache<object?>(TailCacheName).TryAddEntryAsync(operationId, TailCacheName, tailKey, TailEntry(), cancellationToken);
        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException, bool>(pending);
        _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        await owner.AbruptShutdownAsync();
        await cluster.StopNodeAsync("node-a");
        _ = await cluster.StartNodeAsync("node-b", Options(scope, false), cancellationToken);
        _ = await cluster.StartNodeAsync("node-c", Options(scope, false), cancellationToken);
        var restarted = await cluster.StartNodeAsync("node-a", Options(scope, false), cancellationToken);
        return (restarted, tailKey);
    }
}
