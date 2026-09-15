using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Integration tests for multi-node public CRUD and cross-node visibility.</summary>
[Immutable]
public sealed class CrossNodeCrudTests : CrossNodeTestBase
{
    /// <summary>Verifies TryAddAsync(string, T) observes existing named-cache values across nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddLosesToRemoteKey(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-add");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That(await Cluster.CacheB.TryAddAsync(key, "v2", cancellationToken: cancellationToken)).IsFalse();
    }

    /// <summary>Verifies AddAsync(string, T) observes existing named-cache entries across nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddingOnNodeBThrowsForDuplicateFromNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-add-conflict");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(Cluster.CacheB.AddAsync(key, "v2", cancellationToken: cancellationToken));
    }

    /// <summary>Verifies only one concurrent AddAsync succeeds for the same key across nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentAddFromBothNodesOneWinner(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-add");

        var a = TwoNodeSupport.CaptureAddAsync(Cluster.CacheA, key, "a", cancellationToken);
        var b = TwoNodeSupport.CaptureAddAsync(Cluster.CacheB, key, "b", cancellationToken);

        var errors = await Task.WhenAll(a, b);

        _ = await Assert.That(errors).HasSingleItem(static e => e == null);
        _ = await Assert.That(errors).HasSingleItem(static e => e is CacheConflictException);
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsTrue();
    }

    /// <summary>Verifies only one concurrent TryAddAsync returns true for the same key across nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTryAddFromBothNodesOneTrue(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-try-add");

        var a = Cluster.CacheA.TryAddAsync(key, "a", cancellationToken: cancellationToken);
        var b = Cluster.CacheB.TryAddAsync(key, "b", cancellationToken: cancellationToken);

        var results = await Task.WhenAll(a, b);

        _ = await Assert.That(results).HasSingleItem(static r => r);
        _ = await Assert.That(results).HasSingleItem(static r => !r);
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsTrue();
    }

    /// <summary>Verifies concurrent upserts from different nodes converge to one visible value without corrupting reads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentUpsertsLeaveReadableValue(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "concurrent-upsert");

        var tasks = new Task[50];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = i % 2 == 0 ? Cluster.CacheA.SetAsync(key, $"a-{NodeInvariantIndexStrings.Format(i)}", cancellationToken: cancellationToken) : Cluster.CacheB.SetAsync(
                key,
                $"b-{NodeInvariantIndexStrings.Format(i)}",
                cancellationToken: cancellationToken);
        }

        await Task.WhenAll(tasks);

        var valueA = await Cluster.CacheA.GetValueAsync(key, cancellationToken);
        var valueB = await Cluster.CacheB.GetValueAsync(key, cancellationToken);

        _ = await Assert.That(valueA.Found).IsTrue();
        _ = await Assert.That(valueB.Value).IsEqualTo(valueA.Value);
    }

    /// <summary>Verifies an external gRPC client connected to a non-owner node is routed through the server-side cluster pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalClientMutationsOwnedByNodeB(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "external-client-route");
        await using var client = await LoopbackConnect.ConnectAsync(Cluster.NodeAAddress, cancellationToken);
        var cache = await client.GetCacheAsync<object?>("orders", cancellationToken);

        await cache.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v1");
        _ = await Assert.That((await cache.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies GetEntryAsync sees a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetEntryOnNodeBReturnsEntryFromNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-get-entry");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        var entry = await Cluster.CacheB.GetEntryAsync(key, cancellationToken);

        _ = await Assert.That(entry.Found).IsTrue();
    }

    /// <summary>Verifies a stored null value remains distinguishable from a missing key across nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetRemoteNull(CancellationToken cancellationToken)
    {
        await Cluster.CacheA.SetAsync("null-key", null, cancellationToken: cancellationToken);

        var result = await Cluster.CacheB.GetValueAsync("null-key", cancellationToken);

        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That((await Cluster.CacheB.GetValueAsync("missing-null-key", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies GetValueAsync sees a named-cache value written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetRemoteValue(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-get-value");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        var result = await Cluster.CacheB.GetValueAsync(key, cancellationToken);

        _ = await Assert.That(result.Found).IsTrue();
    }

    /// <summary>Verifies GetValueAsync sees a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetValueOnNodeBFindsKeyInsertedOnNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-get-value");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Found).IsTrue();
    }

    /// <summary>Verifies an update through one node is immediately visible when reading through another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InsertNodeAUpdateNodeBReadsBackOnNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "cross-node-update");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);
        await Cluster.CacheB.SetAsync(key, "v2", cancellationToken: cancellationToken);

        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v2");
    }

    /// <summary>Verifies SetAsync(string, T) writes are visible from another node for the same named cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InsertOnNodeAReadsSameValueOnNodeB(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-insert-get");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That((await Cluster.CacheB.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("v1");
    }

    /// <summary>Verifies the same key in different named caches remains isolated across cluster nodes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NamedCachesIsolateSameKeyAcrossNodes(CancellationToken cancellationToken)
    {
        await Cluster.CacheA.SetAsync("same-key", "order-value", cancellationToken: cancellationToken);
        await Cluster.CustomerCacheA.SetAsync("same-key", "customer-value", cancellationToken: cancellationToken);

        _ = await Assert.That((await Cluster.CacheB.GetValueAsync("same-key", cancellationToken)).Value).IsEqualTo("order-value");
        _ = await Assert.That((await Cluster.CustomerCacheB.GetValueAsync("same-key", cancellationToken)).Value).IsEqualTo("customer-value");
    }

    /// <summary>Verifies RemoveAsync can remove a named-cache entry written by another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveDeletesRemote(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-try-remove");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        var result = await Cluster.CacheB.RemoveAsync(key, cancellationToken);

        _ = await Assert.That(result).IsTrue();
    }

    /// <summary>Verifies RemoveAsync on one node removes a named-cache entry written on another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveNodeBDeletesEntryInsertedOnNodeA(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "cross-node-remove-entry");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That(await Cluster.CacheB.RemoveAsync(key, cancellationToken)).IsTrue();
    }

    /// <summary>Verifies a remove through one node makes the key missing through another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveNodeBThenGetOnNodeAReturnsNull(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeB", "cross-node-remove");

        await Cluster.CacheA.SetAsync(key, "v1", cancellationToken: cancellationToken);

        _ = await Assert.That(await Cluster.CacheB.RemoveAsync(key, cancellationToken)).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies remote RemoveAsync removes an entry after it was read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveRemoteAfterRead(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-entry-metadata");

        await Cluster.CacheA.SetAsync(key, "v", cancellationToken: cancellationToken);

        var before = await Cluster.CacheA.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(before.Found).IsTrue();

        var removed = await Cluster.CacheB.RemoveAsync(key, cancellationToken);

        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies remote RemoveAsync removes a stored null value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveRemoteNull(CancellationToken cancellationToken)
    {
        var key = TwoNodeSupport.FindKeyOwnedBy("orders", "nodeA", "remote-try-remove-null");

        await Cluster.CacheA.SetAsync(key, null, cancellationToken: cancellationToken);

        var removed = await Cluster.CacheB.RemoveAsync(key, cancellationToken);

        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await Cluster.CacheA.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }
}
