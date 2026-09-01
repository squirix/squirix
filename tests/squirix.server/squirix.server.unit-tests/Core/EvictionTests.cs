using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Unit tests for cache eviction policies (LRU and FIFO).
/// Verifies that items are evicted according to the configured capacity and policy.
/// </summary>
[Immutable]
public sealed class EvictionTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Lru" /> is active (default),
    /// the least recently used entry is evicted once capacity is exceeded.
    /// </summary>
    [Fact]
    public async Task LruPolicyEvictsLeastRecentlyUsed()
    {
        var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2 }); // Policy defaults to LRU

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Touch "a" to make it most recently used
        Assert.Equal(1, (await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken)).Value);

        // Insert third; should evict least recently used = "b"
        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        Assert.True((await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync(CacheKey.Default("b"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("c"), DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Fifo" /> is active,
    /// the oldest inserted entry is evicted once capacity is exceeded,
    /// regardless of subsequent accesses.
    /// </summary>
    [Fact]
    public async Task FifoPolicyEvictsOldestInserted()
    {
        var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Fifo });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Access should NOT affect FIFO order
        _ = await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken);

        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        // Oldest ("a") should be evicted
        Assert.False((await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("b"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("c"), DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Ensures that re-Set of an existing key refreshes its LRU position so it is not evicted
    /// as the least recently used entry immediately after the write.
    /// </summary>
    [Fact]
    public async Task LruResSetRefreshesPosition()
    {
        var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2 });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Re-Set "a" to refresh its LRU position to most recently used
        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 10 }, DefaultCancellationToken);

        // Insert third; should evict "b" (least recently used), not "a"
        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        var a = await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken);
        Assert.True(a.Found);
        Assert.Equal(10, a.Value);
        Assert.False((await cache.GetValueAsync(CacheKey.Default("b"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("c"), DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Ensures that re-Set of an existing key in FIFO mode does not change eviction order;
    /// the oldest inserted entry is still evicted regardless of re-Set.
    /// </summary>
    [Fact]
    public async Task FifoResSetDoesNotRefreshPosition()
    {
        var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Fifo });

        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync(CacheKey.Default("b"), new NodeCacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Re-Set "a" — should NOT affect FIFO order
        await cache.SetAsync(CacheKey.Default("a"), new NodeCacheEntry<int> { Value = 10 }, DefaultCancellationToken);

        await cache.SetAsync(CacheKey.Default("c"), new NodeCacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        // Oldest ("a") should still be evicted
        Assert.False((await cache.GetValueAsync(CacheKey.Default("a"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("b"), DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync(CacheKey.Default("c"), DefaultCancellationToken)).Found);
    }
}
