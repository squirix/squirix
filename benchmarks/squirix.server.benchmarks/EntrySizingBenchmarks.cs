using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures journal sizing without allocating a payload buffer.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class EntrySizingBenchmarks
{
    private readonly NodeCacheEntry<object?> _entry = new("value");
    private readonly PhysicalCache<object?> _cache = new(TimeProvider.System, new ObjectCacheEntrySizeEstimator());

    /// <summary>Stores an entry whose caller already owns it.</summary>
    [Benchmark]
    public void SetPrepared() => _cache.Set(CacheKey.Default("key"), _entry, CancellationToken.None);

    /// <summary>Measures the encoded entry length.</summary>
    /// <returns>The encoded entry length.</returns>
    [Benchmark]
    public int Measure() => PreparedJournalEntry.MeasureEncodedLength(_entry);
}
