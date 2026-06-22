using System;
using Squirix.Server.Limits;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Benchmark-facing wrappers for entry payload serialization paths on the write pipeline.</summary>
public static class EntryPayloadWritePathBenchmarkSupport
{
    /// <summary>Serializes the entry once using the binary journal entry codec.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length.</returns>
    public static int BinarySerializeOnce(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return EntryPayloadSizeGuard.MeasureSerializedBytes(entry);
    }

    /// <summary>Simulates validation guard plus journal path with two independent binary encodings.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The combined serialized byte length from both passes.</returns>
    public static int BinarySerializeTwice(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var guardBytes = EntryPayloadSizeGuard.MeasureSerializedBytes(entry);
        EntryPayloadSizeGuard.EnsureWithinLimit(entry);
        return guardBytes + entry.PreparedJournalEntryBytes!.Length;
    }

    /// <summary>Simulates reusing one binary encoding for both validation and journal append.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length after validation.</returns>
    public static int SerializeOnceThenLengthCheck(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        EntryPayloadSizeGuard.EnsureWithinLimit(entry);
        return entry.PreparedJournalEntryBytes!.Length;
    }
}
