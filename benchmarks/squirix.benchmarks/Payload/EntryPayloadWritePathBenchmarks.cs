using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.Limits;
using ServerCacheEntry = Squirix.Server.CacheEntry<string>;

namespace Squirix.Benchmarks.Payload;

/// <summary>
/// Measures binary entry payload serialization cost for the write path:
/// one pass (journal only) vs two passes (validation guard + journal) vs reuse (serialize once, length-check only).
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 8)]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class EntryPayloadWritePathBenchmarks
{
    private ServerCacheEntry _entry = new() { Value = string.Empty, Version = 1 };

    /// <summary>Gets or sets the payload profile measured by the current BenchmarkDotNet case.</summary>
    [Params(EntryPayloadProfile.Small256B, EntryPayloadProfile.Medium64KiB, EntryPayloadProfile.Large1MiB, EntryPayloadProfile.NearLimitEntry)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Property annotated with [Params] must have a public setter")]
    public EntryPayloadProfile Profile { get; set; }

    /// <summary>Baseline: journal path encodes entry bytes once before append.</summary>
    /// <returns>Serialized byte length to prevent dead-code elimination.</returns>
    [Benchmark(Baseline = true, Description = "journal only (1x binary encode)")]
    public int BinarySerializeOnce() => EntryPayloadWritePathBenchmarkSupport.BinarySerializeOnce(_entry);

    /// <summary>Current write path: validation guard and journal each encode independently.</summary>
    /// <returns>Combined serialized byte length from both passes.</returns>
    [Benchmark(Description = "guard + journal (2x binary encode)")]
    public int BinarySerializeTwice() => EntryPayloadWritePathBenchmarkSupport.BinarySerializeTwice(_entry);

    /// <summary>Reuse candidate: encode once, validate by length, pass the same bytes to journal append.</summary>
    /// <returns>Serialized byte length after validation.</returns>
    [Benchmark(Description = "reuse payload (1x encode + length check)")]
    public int SerializeOnceThenLengthCheck() => EntryPayloadWritePathBenchmarkSupport.SerializeOnceThenLengthCheck(_entry);

    /// <summary>Builds the entry under test for the selected payload profile.</summary>
    [GlobalSetup]
    public async Task SetupEntryAsync()
    {
        var value = Profile switch
        {
            EntryPayloadProfile.Small256B => new string('x', 256),
            EntryPayloadProfile.Medium64KiB => new string('x', 64 * 1024),
            EntryPayloadProfile.Large1MiB => new string('x', 1024 * 1024),
            EntryPayloadProfile.NearLimitEntry => await EntryLimitKit.CreateNearLimitStringValueAsync().ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unsupported profile: {Profile}"),
        };

        _entry = new ServerCacheEntry { Value = value, Version = 1 };
    }
}
