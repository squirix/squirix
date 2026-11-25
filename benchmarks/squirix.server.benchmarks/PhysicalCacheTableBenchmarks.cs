using System;
using System.Globalization;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures table growth, slot reuse and overwrites on a single local cache writer.</summary>
[MemoryDiagnoser]
public class PhysicalCacheTableBenchmarks
{
    private const int KeyCount = 32_768;
    private const int Overwrites = 4_096;

    /// <summary>Growth begins at 24,577 keys. Another 511 inserts populate destination pages while leaving work for the full batch.</summary>
    private const int GrowingCount = 25_088;
    private readonly NodeCacheEntry<string> _entry = new("12345678");
    private readonly NodeCacheEntry<string> _replacement = new("abcdefgh");
    private readonly CacheKey[] _keys = new CacheKey[KeyCount * 2];
    private PhysicalCache<string> _cache = CreateCache();

    /// <summary>Prepares disjoint sets of short and long keys outside the timed region.</summary>
    [GlobalSetup(Targets = new[] { nameof(InsertThroughGrowth), nameof(RemoveAndRefill) })]
    public void PrepareKeys()
    {
        for (var i = 0; i < _keys.Length; i++)
        {
            var key = i.ToString("X8", CultureInfo.InvariantCulture);
            _keys[i] = CacheKey.Default((i & 1) == 0 ? key : key.PadLeft(64, 'x'));
        }
    }

    /// <summary>Warms the overwrite path before setting up either timed table state.</summary>
    [GlobalSetup(Targets = new[] { nameof(OverwriteDuringGrowth), nameof(OverwriteAfterGrowth) })]
    public void WarmOverwrites()
    {
        PrepareKeys();
        _cache.Set(_keys[0], _entry, CancellationToken.None, 8);
        for (var i = 0; i < 1_024; i++)
            OverwriteBatch(1);
    }

    /// <summary>Starts each remove/refill batch with the original keys present.</summary>
    [IterationSetup(Target = nameof(RemoveAndRefill))]
    public void PrepareRemoval() => Populate(KeyCount);

    /// <summary>Positions the current incremental table partway through a resize.</summary>
    [IterationSetup(Target = nameof(OverwriteDuringGrowth))]
    public void PrepareGrowth() => Populate(GrowingCount);

    /// <summary>Finishes the same resize before measuring the control.</summary>
    [IterationSetup(Target = nameof(OverwriteAfterGrowth))]
    public void PrepareAfterGrowth()
    {
        Populate(GrowingCount);
        for (var i = 0; i < KeyCount; i++)
            _cache.Set(_keys[0], _entry, CancellationToken.None, 8);
    }

    /// <summary>Inserts distinct keys into a fresh table, including its initial allocation and expansions.</summary>
    [Benchmark(OperationsPerInvoke = KeyCount)]
    public void InsertThroughGrowth()
    {
        Populate(KeyCount);
        VerifyEntries(KeyCount, _keys[KeyCount - 1], _entry);
    }

    /// <summary>Removes every original key and fills the cache with disjoint replacements. One operation is a remove/insert pair.</summary>
    /// <exception cref="InvalidOperationException">An original key was missing.</exception>
    [Benchmark(OperationsPerInvoke = KeyCount)]
    public void RemoveAndRefill()
    {
        for (var i = 0; i < KeyCount; i++)
        {
            if (!_cache.Remove(_keys[i], CancellationToken.None))
                throw new InvalidOperationException("An original key was missing.");
        }

        for (var i = KeyCount; i < _keys.Length; i++)
            _cache.Set(_keys[i], _replacement, CancellationToken.None, 8);
        VerifyEntries(KeyCount, _keys[^1], _replacement);
    }

    /// <summary>Overwrites one key while the insertion workload still has buckets to migrate.</summary>
    [Benchmark(OperationsPerInvoke = Overwrites)]
    public void OverwriteDuringGrowth() => OverwriteBatch(GrowingCount);

    /// <summary>Overwrites the same key with the same values after the table has stabilized.</summary>
    [Benchmark(OperationsPerInvoke = Overwrites)]
    public void OverwriteAfterGrowth() => OverwriteBatch(GrowingCount);

    private static PhysicalCache<string> CreateCache() => new(TimeProvider.System, new CacheEntrySizeEstimator<string>());

    private void Populate(int count)
    {
        _cache = CreateCache();
        for (var i = 0; i < count; i++)
            _cache.Set(_keys[i], _entry, CancellationToken.None, 8);
    }

    private void OverwriteBatch(int count)
    {
        var key = _keys[0];
        for (var i = 0; i < Overwrites; i++)
            _cache.Set(key, (i & 1) == 0 ? _entry : _replacement, CancellationToken.None, 8);
        VerifyEntries(count, key, _replacement);
    }

    private void VerifyEntries(int count, CacheKey key, NodeCacheEntry<string> expected)
    {
        ILocalCacheStats stats = _cache;
        if (stats.EntryCount != count || !string.Equals(_cache.GetValue(key, CancellationToken.None).Value, expected.Value, StringComparison.Ordinal))
            throw new InvalidOperationException("Unexpected entry count or stored value after the batch.");
    }
}
