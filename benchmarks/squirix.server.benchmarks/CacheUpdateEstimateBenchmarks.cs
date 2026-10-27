using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
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
    /// <returns>A task that completes when the update finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public async Task<bool> UpdateLargeAsync()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        return await cache.UpdateAsync(LargeKey, _large, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stores a small value under another key, waiting for the cache lock the updater may hold.</summary>
    /// <returns>A task that completes when the set finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public async Task SetSmallAsync()
    {
        var cache = ThrowHelper.Required(_cache, "Benchmark cache was not initialized.");
        await cache.SetAsync(SmallKey, new NodeCacheEntry<object?>("v", 1), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stores the large entry and, when asked, starts the thread that keeps updating it.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
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
        await _cache.SetAsync(LargeKey, new NodeCacheEntry<object?>(_large, 1), CancellationToken.None).ConfigureAwait(false);
        await _cache.SetAsync(SmallKey, new NodeCacheEntry<object?>("v", 1), CancellationToken.None).ConfigureAwait(false);
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
            _ = cache.UpdateAsync(LargeKey, _large, CancellationToken.None).AsTask();
    }
}
