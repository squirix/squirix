using System;
using System.Globalization;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of one local expiry sweep pass under <see cref="CacheExpiryAuthority.LocalClock" />: the work follows the due entries, not the
/// stored ones, so the cost stays flat in <see cref="Stored" /> when nothing is due.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ExpiredEntryRemovalBenchmarks
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private CacheKey[] _keys = [];
    private PhysicalCache<object?>? _cache;

    /// <summary>Gets or sets the number of entries past their deadline at the time of the pass; -1 means every entry.</summary>
    [Params(0, 1024, -1)]
    public int Due { get; set; }

    /// <summary>Gets or sets the number of stored entries, every one with a deadline.</summary>
    [Params(100_000, 1_000_000)]
    public int Stored { get; set; }

    /// <summary>Removes the due entries.</summary>
    /// <returns>The number of entries removed.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public int RemoveExpired() => ThrowHelper.Required(_cache, "Benchmark cache was not initialized.").ExpiredKeys.RemoveExpired();

    /// <summary>Stores the entries: the due ones expire within a minute, the others in a day, then moves the clock past the minute.</summary>
    [IterationSetup]
    public void Setup()
    {
        var clock = new StepClock(Start);
        _cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        _keys = _keys.Length == Stored ? _keys : CreateKeys(Stored);
        var due = Due < 0 ? Stored : Due;
        for (var i = 0; i < Stored; i++)
        {
            var ttl = i < due ? TimeSpan.FromMinutes(1) : TimeSpan.FromDays(1);
            _cache.SetAsync(_keys[i], new NodeCacheEntry<object?>("v", 1, null, ttl), CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        clock.Now += TimeSpan.FromHours(1);
    }

    private static CacheKey[] CreateKeys(int count)
    {
        var keys = new CacheKey[count];
        for (var i = 0; i < count; i++)
            keys[i] = CacheKey.Default("k" + i.ToString(CultureInfo.InvariantCulture));
        return keys;
    }

    private sealed class StepClock : TimeProvider
    {
        internal StepClock(DateTimeOffset now)
        {
            Now = now;
        }

        internal DateTimeOffset Now { get; set; }

        public override DateTimeOffset GetUtcNow() => Now;
    }
}
