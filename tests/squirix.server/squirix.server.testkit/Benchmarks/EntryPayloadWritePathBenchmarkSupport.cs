using System;
using System.Buffers;
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

    /// <summary>Simulates validation guard plus journal path with two independent binary encodings.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The combined serialized byte length from both passes.</returns>
    public static int BinarySerializeTwice(NodeCacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        var guardBytes = prepared.EncodedLength;
        var length = JournalEntryPayload.Encode(in prepared, out var buffer);
        try
        {
            return guardBytes + length;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Simulates the write path: prepare once, guard on prepared length, then pooled encode.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length after validation.</returns>
    public static int SerializeOnceThenLengthCheck(NodeCacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(prepared.EncodedLength);
        var length = JournalEntryPayload.Encode(in prepared, out var buffer);
        try
        {
            return length;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
