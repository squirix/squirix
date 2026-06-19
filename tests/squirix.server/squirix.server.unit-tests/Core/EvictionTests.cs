using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Unit tests for cache eviction policies (LRU and FIFO).
/// Verifies that items are evicted according to the configured capacity and policy.
/// </summary>
public sealed class EvictionTests : UnitTestBase
{
    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Lru" /> is active (default),
    /// the least recently used entry is evicted once capacity is exceeded.
    /// </summary>
    [Fact]
    public async Task DefaultPolicyIsLruWhenCapacitySetShouldEvictLeastRecentlyUsed()
    {
        await using var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2 }); // Policy defaults to LRU

        await cache.SetAsync("a", new CacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync("b", new CacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Touch "a" to make it most recently used
        Assert.Equal(1, (await cache.GetValueAsync("a", DefaultCancellationToken)).Value);

        // Insert third; should evict least recently used = "b"
        await cache.SetAsync("c", new CacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        Assert.True((await cache.GetValueAsync("a", DefaultCancellationToken)).Found);
        Assert.False((await cache.GetValueAsync("b", DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync("c", DefaultCancellationToken)).Found);
    }

    /// <summary>
    /// Ensures that when <see cref="EvictionPolicyType.Fifo" /> is active,
    /// the oldest inserted entry is evicted once capacity is exceeded,
    /// regardless of subsequent accesses.
    /// </summary>
    [Fact]
    public async Task FifoPolicyWhenCapacitySetShouldEvictOldestInserted()
    {
        await using var cache = new PhysicalCache<int>(null, new EvictionOptions { Capacity = 2, Policy = EvictionPolicyType.Fifo });

        await cache.SetAsync("a", new CacheEntry<int> { Value = 1 }, DefaultCancellationToken);
        await cache.SetAsync("b", new CacheEntry<int> { Value = 2 }, DefaultCancellationToken);

        // Access should NOT affect FIFO order
        _ = await cache.GetValueAsync("a", DefaultCancellationToken);

        await cache.SetAsync("c", new CacheEntry<int> { Value = 3 }, DefaultCancellationToken);

        // Oldest ("a") should be evicted
        Assert.False((await cache.GetValueAsync("a", DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync("b", DefaultCancellationToken)).Found);
        Assert.True((await cache.GetValueAsync("c", DefaultCancellationToken)).Found);
    }
}
