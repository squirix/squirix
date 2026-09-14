using System;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Benchmark-facing wrappers for entry payload serialization paths on the write pipeline.</summary>
public static class EntryPayloadWritePathBenchmarkSupport
{
    /// <summary>Serializes the entry once using the binary journal entry codec.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length.</returns>
    public static int BinarySerializeOnce(NodeCacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return JournalEntryPayload.MeasureSerializedBytes(entry);
    }
}
