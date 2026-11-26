using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Binary snapshot write cost at large entry counts with small scalar values and JSON values.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class SnapshotWriteScaleBenchmarks
{
    private const int JsonVariantCount = 16;

    private TempDirectory? _dir;
    private List<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)>? _items;
    private int _nextIndex;
    private SnapshotWriter? _writer;

    /// <summary>Gets or sets the number of entries per snapshot.</summary>
    [Params(100_000, 1_000_000)]
    public int EntryCount { get; set; }

    /// <summary>Gets or sets a value indicating whether values are JSON objects instead of small scalars.</summary>
    [Params(false, true)]
    public bool JsonValues { get; set; }

    /// <summary>Disposes the temporary data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _dir?.Dispose();
        _dir = null;
        _items = null;
    }

    /// <summary>Builds the entries and the snapshot writer.</summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = new TempDirectory("snapshot-write-scale");
        _writer = new SnapshotWriter(_dir);
        var jsonValues = new StoredValue[JsonVariantCount];
        for (var i = 0; i < jsonValues.Length; i++)
            jsonValues[i] = StoredValue.FromJson(System.Text.Encoding.UTF8.GetBytes($$"""{"id":{{i}},"name":"item-{{i}}","tags":["a","b"],"price":{{i}}.5}"""));

        var items = new List<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)>(EntryCount);
        for (var i = 0; i < EntryCount; i++)
        {
            var value = JsonValues
                ? jsonValues[i % JsonVariantCount]
                : (i % 3) switch
                {
                    0 => StoredValue.FromText($"value-{NodeInvariantIndexStrings.Format(i)}"),
                    1 => StoredValue.FromInteger(i),
                    _ => StoredValue.FromDouble(i * 1.5d),
                };
            items.Add((CacheKey.Default($"key-{NodeInvariantIndexStrings.Format(i)}"), new NodeCacheEntry<StoredValue> { Value = value, Version = 1 }));
        }

        _items = items;
    }

    /// <summary>Writes and publishes one full snapshot.</summary>
    /// <returns>A task that completes when the snapshot is published and removed.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public async Task WriteSnapshotAsync()
    {
        var writer = ThrowHelper.Required(_writer, "Benchmark writer was not initialized.");
        var items = ThrowHelper.Required(_items, "Benchmark items were not initialized.");
        var path = await writer.WriteAsync(++_nextIndex, items, [], CancellationToken.None).ConfigureAwait(false);
        File.Delete(path);
    }
}
