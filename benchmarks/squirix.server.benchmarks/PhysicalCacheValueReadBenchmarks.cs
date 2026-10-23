using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of a value hit on a <see cref="PhysicalCache{T}" /> without a capacity, where the read takes no lock, and on one with a capacity
/// and the LRU policy, where the read moves the entry to the front of the eviction order under the lock. Both are expected to allocate
/// nothing. The CI allocation guard checks these allocations against alloc-ci-baseline.json.
/// </summary>
[MemoryDiagnoser]
public class PhysicalCacheValueReadBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private const int LruCapacity = 2;
    private static readonly CacheKey Key = CacheKey.Default("k");
    private static readonly CacheKey OtherKey = CacheKey.Default("l");
    private PhysicalCache<string>? _cache;
    private PhysicalCache<string>? _lruCache;

    /// <summary>Reads the value of the stored entry.</summary>
    /// <returns>A task that completes when all reads finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ValueHitAsync()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await cache.GetValueAsync(Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads the values of two entries of an LRU cache in turn, so every read moves the entry it hits to the front.</summary>
    /// <returns>A task that completes when all reads finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task LruValueHitAsync()
    {
        var cache = ThrowHelper.Required(_lruCache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await cache.GetValueAsync((i & 1) == 0 ? Key : OtherKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stores the entries the reads hit.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        await _cache.SetAsync(Key, Entry(), CancellationToken.None).ConfigureAwait(false);

        _lruCache = new PhysicalCache<string>(
            TimeProvider.System,
            new CacheEntrySizeEstimator<string>(),
            new EvictionOptions { Capacity = LruCapacity, Policy = EvictionPolicyType.Lru });
        await _lruCache.SetAsync(Key, Entry(), CancellationToken.None).ConfigureAwait(false);
        await _lruCache.SetAsync(OtherKey, Entry(), CancellationToken.None).ConfigureAwait(false);
    }

    private static NodeCacheEntry<string> Entry() => new("v", 1, null, TimeSpan.FromDays(1));
}
