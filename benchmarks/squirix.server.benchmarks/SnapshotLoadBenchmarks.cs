using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot.Binary;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of loading a binary snapshot of small entries and of JSON-valued entries.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class SnapshotLoadBenchmarks
{
    private const int JsonEntryCount = 100_000;
    private const int SmallEntryCount = 100_000;
    private const int LargeEntryCount = 1_000_000;
    private const string JsonText = """{"a":1,"b":[true,null,"x"],"c":{"d":2.5}}""";
    private string _dataDir = string.Empty;
    private string _jsonPath = string.Empty;
    private string _largePath = string.Empty;
    private string _smallPath = string.Empty;

    /// <summary>Writes the snapshots with the production writer.</summary>
    /// <returns>A task that completes when the snapshots are written.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dataDir = Path.Join(Path.GetTempPath(), "squirix-snapshot-load-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(_dataDir);
        var writer = new SnapshotWriter(_dataDir);
        _smallPath = await writer.WriteAsync(1, BuildSmall(SmallEntryCount), [], CancellationToken.None).ConfigureAwait(false);
        _largePath = await writer.WriteAsync(2, BuildSmall(LargeEntryCount), [], CancellationToken.None).ConfigureAwait(false);
        _jsonPath = await writer.WriteAsync(3, BuildJson(JsonEntryCount), [], CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Deletes the scratch directory.</summary>
    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dataDir, true);

    /// <summary>Loads 100k small entries.</summary>
    /// <returns>The number of loaded entries.</returns>
    [Benchmark]
    public async Task<int> LoadSmall100KAsync() => (await StoreFactory.CreateReader().LoadStrictAsync<object?>(_smallPath, null, CancellationToken.None).ConfigureAwait(false)).Entries.Count;

    /// <summary>Loads 1M small entries.</summary>
    /// <returns>The number of loaded entries.</returns>
    [Benchmark]
    public async Task<int> LoadSmall1MAsync() => (await StoreFactory.CreateReader().LoadStrictAsync<object?>(_largePath, null, CancellationToken.None).ConfigureAwait(false)).Entries.Count;

    /// <summary>Loads 100k JSON-valued entries.</summary>
    /// <returns>The number of loaded entries.</returns>
    [Benchmark]
    public async Task<int> LoadJson100KAsync() => (await StoreFactory.CreateReader().LoadStrictAsync<object?>(_jsonPath, null, CancellationToken.None).ConfigureAwait(false)).Entries.Count;

    private static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildJson(int count)
    {
        using var document = JsonDocument.Parse(JsonText);
        var value = document.RootElement.Clone();
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(count);
        for (var i = 0; i < count; i++)
            items.Add((CacheKey.Default($"key-{i}"), new NodeCacheEntry<object?> { Value = value, Version = 1 }));

        return items;
    }

    private static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildSmall(int count)
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(count);
        for (var i = 0; i < count; i++)
            items.Add((CacheKey.Default($"key-{i}"), new NodeCacheEntry<object?> { Value = $"value-{i}", Version = 1 }));

        return items;
    }
}
