using System;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using Squirix.Server.TestKit.Limits;
using ServerCacheEntry = Squirix.Server.CacheEntry<string>;

namespace Squirix.Benchmarks;

/// <summary>
/// Measures discriminated entry JSON serialization cost for the write path:
/// one pass (journal only) vs two passes (validation guard + journal) vs reuse (serialize once, length-check only).
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 8)]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class EntryPayloadWritePathBenchmarks
{
    private ServerCacheEntry _entry = null!;

    /// <summary>
    /// Gets or sets the payload profile measured by the current BenchmarkDotNet case.
    /// </summary>
    [Params(EntryPayloadProfile.Small256B, EntryPayloadProfile.Medium64KiB, EntryPayloadProfile.Large1MiB, EntryPayloadProfile.NearLimitDiscriminated)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Property annotated with [Params] must have a public setter")]
    public EntryPayloadProfile Profile { get; set; }

    /// <summary>
    /// Baseline: journal path serializes discriminated entry JSON once before append.
    /// </summary>
    /// <returns>Serialized byte length to prevent dead-code elimination.</returns>
    [Benchmark(Baseline = true, Description = "journal only (1x discriminated serialize)")]
    public int DiscriminatedSerializeOnce() => EntryPayloadWritePathBenchmarkSupport.DiscriminatedSerializeOnce(_entry);

    /// <summary>
    /// Current write path: validation guard and journal each build discriminated JSON independently.
    /// </summary>
    /// <returns>Combined serialized byte length from both passes.</returns>
    [Benchmark(Description = "guard + journal (2x discriminated serialize)")]
    public int DiscriminatedSerializeTwice() => EntryPayloadWritePathBenchmarkSupport.DiscriminatedSerializeTwice(_entry);

    /// <summary>
    /// Reuse candidate: serialize once, validate by length, pass the same bytes to journal append.
    /// </summary>
    /// <returns>Serialized byte length after validation.</returns>
    [Benchmark(Description = "reuse payload (1x serialize + length check)")]
    public int SerializeOnceThenLengthCheck() => EntryPayloadWritePathBenchmarkSupport.SerializeOnceThenLengthCheck(_entry);

    /// <summary>
    /// Builds the entry under test for the selected payload profile.
    /// </summary>
    [GlobalSetup]
    public void SetupEntry()
    {
        var value = Profile switch
        {
            EntryPayloadProfile.Small256B => new string('x', 256),
            EntryPayloadProfile.Medium64KiB => new string('x', 64 * 1024),
            EntryPayloadProfile.Large1MiB => new string('x', 1024 * 1024),
            EntryPayloadProfile.NearLimitDiscriminated => EntryPayloadLimitTestHelpers.CreateNearLimitDiscriminatedStringValue(),
            _ => throw new InvalidOperationException($"Unsupported profile: {Profile}"),
        };

        _entry = new ServerCacheEntry { Value = value, Version = 1 };
    }
}
