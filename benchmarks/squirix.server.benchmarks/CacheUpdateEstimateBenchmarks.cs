using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of updating a cache entry with a moderately large JSON object value on a cache without persistence, alone and while another writer
/// stores small values: the estimate of the updated value is the part of an update that may hold the cache lock.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class CacheUpdateEstimateBenchmarks
{
    private const int PropertyCount = 40;
    private const int PropertyValueLength = 40;
    private static readonly CacheKey LargeKey = CacheKey.Default("large");
    private static readonly CacheKey SmallKey = CacheKey.Default("small");
    private PhysicalCache<object?>? _cache;
    private object? _large;
    private Thread? _updater;
    private volatile bool _stopRequested;

    /// <summary>Gets or sets a value indicating whether another thread keeps updating the large value while the set is measured.</summary>
    [Params(false, true)]
    public bool ConcurrentUpdater { get; set; }

    /// <summary>Updates the large value from this thread.</summary>
    /// <returns>Whether the entry was updated.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public bool UpdateLarge()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        return cache.Update(LargeKey, _large, CancellationToken.None);
    }

    /// <summary>Stores a small value under another key, waiting for the cache lock the updater may hold.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public void SetSmall()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        cache.Set(SmallKey, new NodeCacheEntry<object?>("v"), CancellationToken.None);
    }

    /// <summary>Stores the large entry and, when asked, starts the thread that keeps updating it.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var json = new StringBuilder("{");
        for (var i = 0; i < PropertyCount; i++)
        {
            if (i > 0)
                _ = json.Append(',');

            _ = json.Append(CultureInfo.InvariantCulture, $"\"property{i}\":\"{new string('x', PropertyValueLength)}\"");
        }

        _ = json.Append('}');
        using var document = JsonDocument.Parse(json.ToString());
        _large = document.RootElement.Clone();
        _cache = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        _cache.Set(LargeKey, new NodeCacheEntry<object?>(_large), CancellationToken.None);
        _cache.Set(SmallKey, new NodeCacheEntry<object?>("v"), CancellationToken.None);
        if (!ConcurrentUpdater)
            return;

        _updater = new Thread(UpdateUntilStopped) { IsBackground = true };
        _updater.Start();
    }

    /// <summary>Stops the updating thread.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _stopRequested = true;
        _updater?.Join();
    }

    private void UpdateUntilStopped()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        while (!_stopRequested)
            _ = cache.Update(LargeKey, _large, CancellationToken.None);
    }
}
