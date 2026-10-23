using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Measures the managed heap one stored entry costs in the physical cache, with and without a deadline, against the per-entry estimate of
/// <see cref="CacheEntrySizeEstimator{T}" />, which this calibrates. Every entry shares one value and the key strings are built before the
/// baseline, so the growth is the store structure: the node with its key, the bucket slots and, with a deadline, the deadline index. A console
/// runner rather than BenchmarkDotNet because it reads the heap after a full collection, not the allocations of an operation.
/// </summary>
internal static class EntryMemoryMeasurement
{
    private const int DefaultEntries = 1_000_000;

    /// <summary>A deadline far enough that no entry expires during the measurement.</summary>
    private static readonly TimeSpan Ttl = TimeSpan.FromDays(1);

    private static readonly bool[] DeadlineModes = [false, true];

    /// <summary>Runs a warmup round and a measured round per deadline mode and prints a Markdown table.</summary>
    /// <param name="args">Options: --entries the number of entries per round.</param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task RunAsync(string[] args)
    {
        var entries = MeasurementArgs.Parse(args).GetInt("--entries", DefaultEntries);
        MeasurementOutput.Line($"Entry memory measurement: entries={entries.ToString(CultureInfo.InvariantCulture)}");
        var rows = new List<string>();
        foreach (var withDeadline in DeadlineModes)
        {
            _ = await MeasureAsync(entries, withDeadline).ConfigureAwait(false);
            rows.Add(await MeasureAsync(entries, withDeadline).ConfigureAwait(false));
        }

        MeasurementOutput.Table(
            "Managed heap per stored entry (bytes)",
            "| deadline | heap growth | estimate |",
            rows);
    }

    private static async Task<string> MeasureAsync(int entries, bool withDeadline)
    {
        var keys = new CacheKey[entries];
        for (var i = 0; i < entries; i++)
            keys[i] = CacheKey.Default("k" + i.ToString("D7", CultureInfo.InvariantCulture));

        object value = "v";
        var before = HeapAfterFullCollection();
        var cache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>());
        for (var i = 0; i < entries; i++)
        {
            var entry = withDeadline ? new NodeCacheEntry<object?>(value, 1, null, Ttl) : new NodeCacheEntry<object?>(value);
            await cache.SetAsync(keys[i], entry, CancellationToken.None).ConfigureAwait(false);
        }

        double entryCount = entries;
        var growth = (HeapAfterFullCollection() - before) / entryCount;
        ILocalCacheStats stats = cache;
        var estimate = stats.EstimatedBytes / entryCount;
        GC.KeepAlive(cache);
        GC.KeepAlive(keys);
        return $"| {(withDeadline ? "yes" : "no")} | {MeasurementOutput.Fixed2(growth)} | {MeasurementOutput.Fixed2(estimate)} |";
    }

    /// <summary>Reads the managed heap size after a full blocking collection, so it counts only live objects.</summary>
    /// <returns>The heap size in bytes.</returns>
    private static long HeapAfterFullCollection() =>
#pragma warning disable S1215 // The measurement is the live heap, which only a full collection isolates from garbage.
        GC.GetTotalMemory(true);
#pragma warning restore S1215
}
