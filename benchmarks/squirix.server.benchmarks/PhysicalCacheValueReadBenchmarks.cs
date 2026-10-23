using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of a value hit on a <see cref="PhysicalCache{T}" /> without a capacity: the read takes no lock and is expected to allocate nothing.
/// The CI allocation guard checks these allocations against alloc-ci-baseline.json.
/// </summary>
[MemoryDiagnoser]
public class PhysicalCacheValueReadBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private static readonly CacheKey Key = CacheKey.Default("k");
    private PhysicalCache<string>? _cache;

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

    /// <summary>Stores the entry the reads hit.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        await _cache.SetAsync(Key, new NodeCacheEntry<string>("v", 1, null, TimeSpan.FromDays(1)), CancellationToken.None).ConfigureAwait(false);
    }
}
