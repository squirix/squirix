using System;
using System.Buffers;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    internal static PooledJournalPayload Encode(in PreparedJournalEntry prepared)
    {
        var pooledBuffer = ArrayPool<byte>.Shared.Rent(prepared.EncodedLength);
        try
        {
            CacheEntryCodec.Write(prepared.ObjectEntry, pooledBuffer);
            return new PooledJournalPayload(pooledBuffer, prepared.EncodedLength);
        }
        catch
        {
            ArrayPool<byte>.Shared.ReturnCleared(pooledBuffer);
            throw;
        }
    }

    internal static void EnsureEncodedLengthWithinLimit<T>(NodeCacheEntry<T> entry)
    {
        EntryTagsGuard.EnsureWithinLimits(entry.Tags);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(ComputeEncodedLength(entry));
    }

    internal static int MeasureSerializedBytes<T>(NodeCacheEntry<T> entry) => ComputeEncodedLength(entry);

    internal static PreparedJournalEntry PrepareEncode<T>(NodeCacheEntry<T> entry) => PreparedJournalEntry.From(entry);

    internal static bool TryDecode<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out _))
            return true;
        entry = null;
        return false;
    }

    private static int ComputeEncodedLength<T>(NodeCacheEntry<T> entry) => PrepareEncode(entry).EncodedLength;
}
