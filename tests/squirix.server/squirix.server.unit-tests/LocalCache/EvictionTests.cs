using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>
/// Unit tests for cache eviction policies (LRU and FIFO).
/// Verifies that items are evicted according to the configured capacity and policy.
/// </summary>
[Immutable]
public sealed class EvictionTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Fifo" /> is active,
    /// the oldest inserted entry is evicted once capacity is exceeded,
    /// regardless of subsequent accesses.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FifoPolicyEvictsOldestInserted(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<int>(TimeProvider.System, new CacheEntrySizeEstimator<int>(), new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Fifo });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, cancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, cancellationToken);

        // Access should NOT affect FIFO order
        _ = await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken);

        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, cancellationToken);

        // Oldest ("a") should be evicted
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("b"), cancellationToken)).Found).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("c"), cancellationToken)).Found).IsTrue();
    }

    /// <summary>
    /// Ensures that re-Set of an existing key in FIFO mode does not change eviction order;
    /// the oldest inserted entry is still evicted regardless of re-Set.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FifoResSetDoesNotRefreshPosition(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<int>(TimeProvider.System, new CacheEntrySizeEstimator<int>(), new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Fifo });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, cancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, cancellationToken);

        // Re-Set "a" — should NOT affect FIFO order
        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 10 }, cancellationToken);

        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, cancellationToken);

        // Oldest ("a") should still be evicted
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("b"), cancellationToken)).Found).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("c"), cancellationToken)).Found).IsTrue();
    }

    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Lru" /> is active (default),
    /// the least recently used entry is evicted once capacity is exceeded.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LruPolicyEvictsLeastRecentlyUsed(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<int>(TimeProvider.System, new CacheEntrySizeEstimator<int>(), new EvictionOptions { Capacity = 2 }); // Policy defaults to LRU

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, cancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, cancellationToken);

        // Touch "a" to make it most recently used
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Value).IsEqualTo(1);

        // Insert third; should evict least recently used = "b"
        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken)).Found).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("b"), cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("c"), cancellationToken)).Found).IsTrue();
    }

    /// <summary>
    /// Ensures that re-Set of an existing key refreshes its LRU position so it is not evicted
    /// as the least recently used entry immediately after the write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LruResSetRefreshesPosition(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<int>(TimeProvider.System, new CacheEntrySizeEstimator<int>(), new EvictionOptions { Capacity = 2 });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, cancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, cancellationToken);

        // Re-Set "a" to refresh its LRU position to most recently used
        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 10 }, cancellationToken);

        // Insert third; should evict "b" (least recently used), not "a"
        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, cancellationToken);

        var a = await cache.GetValueAsync(CacheKey.Default("a"), cancellationToken);
        _ = await Assert.That(a.Found).IsTrue();
        _ = await Assert.That(a.Value).IsEqualTo(10);
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("b"), cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("c"), cancellationToken)).Found).IsTrue();
    }

    /// <summary>
    /// Ensures that an LRU hit moves the entry to the front of the eviction order without allocating, and that the order it leaves still
    /// evicts the least recently used entry.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LruHitMovesEntryWithoutAllocating(CancellationToken cancellationToken)
    {
        const int calls = 1000;
        var a = CacheKey.Default("a");
        var b = CacheKey.Default("b");
        var cache = new PhysicalCache<int>(TimeProvider.System, new CacheEntrySizeEstimator<int>(), new EvictionOptions { Capacity = 2 });
        await cache.SetAsync(a, new NodeCacheEntry<int> { Value = 1 }, cancellationToken);
        await cache.SetAsync(b, new NodeCacheEntry<int> { Value = 2 }, cancellationToken);
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync((i & 1) == 0 ? a : b, cancellationToken);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync((i & 1) == 0 ? a : b, cancellationToken);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // "b" was read last, so "a" is the least recently used.
        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, cancellationToken);

        _ = await Assert.That(allocated).IsEqualTo(0L);
        _ = await Assert.That((await cache.GetValueAsync(a, cancellationToken)).Found).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(b, cancellationToken)).Found).IsTrue();
    }
}
