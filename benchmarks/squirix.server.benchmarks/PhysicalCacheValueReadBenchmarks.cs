using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of a value hit through the logical local-cache contract, backed by an unbounded physical cache
/// or a bounded LRU cache. Both paths are expected to allocate nothing per read.
/// </summary>
[MemoryDiagnoser]
public class PhysicalCacheValueReadBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private const int LruCapacity = 2;
    private static readonly CacheKey Key = CacheKey.Default("k");
    private static readonly CacheKey OtherKey = CacheKey.Default("l");
    private ClientCache<string>? _cache;
    private ClientCache<string>? _lruCache;

    /// <summary>Reads the value of the stored entry.</summary>
    /// <returns>The read operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ValueHitAsync()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await cache.GetValueAsync(Key.Namespace, Key.Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads two entries in turn, moving each hit to the front of the LRU eviction order.</summary>
    /// <returns>The read operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task LruValueHitAsync()
    {
        var cache = ThrowHelper.Required(_lruCache, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var key = (i & 1) == 0 ? Key : OtherKey;
            _ = await cache.GetValueAsync(key.Namespace, key.Key, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Stores the entries the reads hit.</summary>
    /// <returns>The setup operation.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var physical = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        _cache = new ClientCache<string>(physical, physical);
        await _cache.SetEntryAsync("setup", Key.Namespace, Key.Key, Entry(), CancellationToken.None).ConfigureAwait(false);

        var lruPhysical = new PhysicalCache<string>(
            TimeProvider.System,
            new CacheEntrySizeEstimator<string>(),
            new EvictionOptions { Capacity = LruCapacity, Policy = EvictionPolicyType.Lru });
        _lruCache = new ClientCache<string>(lruPhysical, lruPhysical);
        await _lruCache.SetEntryAsync("setup", Key.Namespace, Key.Key, Entry(), CancellationToken.None).ConfigureAwait(false);
        await _lruCache.SetEntryAsync("setup", OtherKey.Namespace, OtherKey.Key, Entry(), CancellationToken.None).ConfigureAwait(false);
    }

    private static NodeCacheEntry<string> Entry() => new("v", 1, null, TimeSpan.FromDays(1));
}
