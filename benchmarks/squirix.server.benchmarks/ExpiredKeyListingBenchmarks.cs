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
/// Cost of listing the expired keys from the deadline index under <see cref="CacheExpiryAuthority.CommittedRecords" /> (#1031): the work and
/// the allocations follow the due keys, not the stored entries, so the cost stays flat as <see cref="Stored" /> grows.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ExpiredKeyListingBenchmarks
{
    private const int MaxListed = 1024;
    private PhysicalCache<object?>? _cache;
    private DateTime _listedAt;

    /// <summary>Gets or sets the number of keys past their deadline at the time of the listing.</summary>
    [Params(0, 64)]
    public int Due { get; set; }

    /// <summary>Gets or sets the number of stored entries, every one with a deadline.</summary>
    [Params(1_000, 100_000)]
    public int Stored { get; set; }

    /// <summary>Lists the due keys; a listing removes nothing, so every invocation lists the same keys.</summary>
    /// <returns>The number of keys listed.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public int ListExpired() => ThrowHelper.Required(_cache, "Benchmark cache was not initialized.").ExpiredKeys.ListExpired(_listedAt, MaxListed, static _ => true).Count;

    /// <summary>Stores the entries: <see cref="Due" /> of them expire within a minute, the others in a day.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _cache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        for (var i = 0; i < Stored; i++)
        {
            var ttl = i < Due ? TimeSpan.FromMinutes(1) : TimeSpan.FromDays(1);
            var key = CacheKey.Default("k" + i.ToString(CultureInfo.InvariantCulture));
            await _cache.SetAsync(key, new NodeCacheEntry<object?>("v", 1, null, ttl), CancellationToken.None).ConfigureAwait(false);
        }

        _listedAt = TimeProvider.System.GetUtcNow().UtcDateTime.AddHours(1);
    }
}
