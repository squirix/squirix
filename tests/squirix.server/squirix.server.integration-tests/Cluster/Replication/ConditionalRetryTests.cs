using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>A retried conditional write on an RF=3 group gets the outcome of its first attempt through the whole cache pipeline.</summary>
public sealed class ConditionalRetryTests : NodeIntegrationTestBase
{
    private const string CacheName = "conditional-retry";

    /// <summary>Bounds the verification of a fresh cluster.</summary>
    private static readonly TimeSpan VerificationBound = TimeSpan.FromSeconds(30);

    /// <summary>A retried add that succeeded replays <see langword="true" /> although its key now exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedAddReplaysTrue(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("conditional-retry-add"), cancellationToken);
        var (cache, key) = await OwnedCacheAsync(cluster, cancellationToken);
        var operationId = Guid.NewGuid().ToString("N");

        var added = await cache.TryAddEntryAsync(operationId, CacheName, key, Entry("first"), cancellationToken);
        var retried = await cache.TryAddEntryAsync(operationId, CacheName, key, Entry("first"), cancellationToken);
        var other = await cache.TryAddEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, Entry("second"), cancellationToken);

        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That(retried).IsTrue();
        _ = await Assert.That(other).IsFalse();
    }

    /// <summary>A retried add that was refused replays <see langword="false" /> after its key is removed, instead of adding it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedRefusedAddStaysRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("conditional-retry-refused"), cancellationToken);
        var (cache, key) = await OwnedCacheAsync(cluster, cancellationToken);
        var operationId = Guid.NewGuid().ToString("N");
        _ = await cache.TryAddEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, Entry("winner"), cancellationToken);

        var refused = await cache.TryAddEntryAsync(operationId, CacheName, key, Entry("loser"), cancellationToken);
        _ = await cache.RemoveAsync(Guid.NewGuid().ToString("N"), CacheName, key, cancellationToken);
        var retried = await cache.TryAddEntryAsync(operationId, CacheName, key, Entry("loser"), cancellationToken);
        var present = (await cache.GetValueAsync(CacheName, key, cancellationToken)).Found;

        _ = await Assert.That(refused).IsFalse();
        _ = await Assert.That(retried).IsFalse();
        _ = await Assert.That(present).IsFalse();
    }

    /// <summary>A retried set of an absent key is accepted as the same operation: the first attempt was recorded as a set too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetriedSetKeepsIdentity(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", Options("conditional-retry-set"), cancellationToken);
        var (cache, key) = await OwnedCacheAsync(cluster, cancellationToken);
        var operationId = Guid.NewGuid().ToString("N");

        await cache.SetEntryAsync(operationId, CacheName, key, Entry("value"), cancellationToken);
        await cache.SetEntryAsync(operationId, CacheName, key, Entry("value"), cancellationToken);
        var stored = await cache.GetValueAsync(CacheName, key, cancellationToken);

        _ = await Assert.That(stored.Found).IsTrue();
        _ = await Assert.That(stored.Value).IsEqualTo("value");
    }

    private static IntegrationStartOptions Options(string scope) => new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = scope };

    private static NodeCacheEntry<object?> Entry(string value) => new() { Value = value, Version = 1 };

    /// <summary>Returns node-a's cache pipeline and a key node-a owns, once node-a's replica slots are verified.</summary>
    /// <param name="cluster">The running RF=3 cluster.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The pipeline and the key.</returns>
    private static async Task<(ILogicalNamespacedCache<object?> Cache, string Key)> OwnedCacheAsync(
        TestCluster<IntegrationStartOptions> cluster,
        CancellationToken cancellationToken)
    {
        var owner = cluster["node-a"];
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(VerificationBound);
        var committer = owner.GetRequiredService<ReplicaGroupCommitter>();
        while (await committer.VerifyReplicasAsync(deadline.Token) != ReplicaVerification.AllReady)
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, deadline.Token);

        return (owner.GetCache<object?>(CacheName), owner.FindKeyOwnedBy(CacheName, "node-a"));
    }
}
