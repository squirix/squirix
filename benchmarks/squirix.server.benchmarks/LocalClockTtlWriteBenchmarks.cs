using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of a write with a time to live on a cache whose local clock decides expiry: every overwrite moves the deadline, so it is recorded
/// in the deadline index, and the index is rebuilt once stale items pile up.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class LocalClockTtlWriteBenchmarks
{
    private const int WritesPerInvocation = 100_000;
    private CacheKey[] _keys = [];
    private PhysicalCache<object?>? _cache;

    /// <summary>Gets or sets the number of stored entries, every one with a deadline.</summary>
    [Params(100_000, 1_000_000)]
    public int Stored { get; set; }

    /// <summary>Overwrites stored entries, each with a new deadline.</summary>
    /// <returns>A task that completes when the writes finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = WritesPerInvocation)]
    public async Task SetWithTtlAsync()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        var ttl = TimeSpan.FromDays(1);
        for (var i = 0; i < WritesPerInvocation; i++)
            await cache.SetAsync(_keys[i % _keys.Length], new NodeCacheEntry<object?>("v", 1, null, ttl), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stores the entries, each with a deadline a day away.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _cache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>());
        _keys = new CacheKey[Stored];
        for (var i = 0; i < Stored; i++)
        {
            _keys[i] = CacheKey.Default("k" + i.ToString(CultureInfo.InvariantCulture));
            await _cache.SetAsync(_keys[i], new NodeCacheEntry<object?>("v", 1, null, TimeSpan.FromDays(1)), CancellationToken.None).ConfigureAwait(false);
        }
    }
}
