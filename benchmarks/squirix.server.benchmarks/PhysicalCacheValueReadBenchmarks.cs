using System;
using System.Threading;
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
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void ValueHit()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = cache.GetValue(Key, CancellationToken.None);
    }

    /// <summary>Reads the values of two entries of an LRU cache in turn, so every read moves the entry it hits to the front.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void LruValueHit()
    {
        var cache = ThrowHelper.Required(_lruCache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = cache.GetValue((i & 1) == 0 ? Key : OtherKey, CancellationToken.None);
    }

    /// <summary>Stores the entries the reads hit.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        _cache.Set(Key, Entry(), CancellationToken.None);

        _lruCache = new PhysicalCache<string>(
            TimeProvider.System,
            new CacheEntrySizeEstimator<string>(),
            new EvictionOptions { Capacity = LruCapacity, Policy = EvictionPolicyType.Lru });
        _lruCache.Set(Key, Entry(), CancellationToken.None);
        _lruCache.Set(OtherKey, Entry(), CancellationToken.None);
    }

    private static NodeCacheEntry<string> Entry() => new("v", 1, null, TimeSpan.FromDays(1));
}
