using System;
using System.Buffers;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Entries;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    public static int ComputeEncodedLength<T>(CacheEntry<T> entry) => PrepareEncode(entry).EncodedLength;

    public static PreparedJournalEntry PrepareEncode<T>(CacheEntry<T> entry) => PreparedJournalEntry.From(entry);

    public static byte[] Encode<T>(CacheEntry<T> entry)
    {
        var prepared = PrepareEncode(entry);
        return BufferEx.EncodeToOwned(
            prepared.EncodedLength,
            prepared.ObjectEntry,
            static (objectEntry, span) => CacheEntryCodec.Write(objectEntry, span));
    }

    /// <summary>
    /// Encodes the entry into a buffer rented from <see cref="ArrayPool{T}.Shared" />. The rented array is
    /// oversized, so callers must use the returned logical length and return <paramref name="pooledBuffer" />
    /// to the shared pool exactly once when done.
    /// </summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to encode.</param>
    /// <param name="pooledBuffer">The rented buffer holding the encoded bytes (length may exceed the logical payload).</param>
    /// <returns>The logical length of the encoded payload within <paramref name="pooledBuffer" />.</returns>
    public static int Encode<T>(CacheEntry<T> entry, out byte[] pooledBuffer) => Encode(PrepareEncode(entry), out pooledBuffer);

    public static int Encode(in PreparedJournalEntry prepared, out byte[] pooledBuffer)
    {
        pooledBuffer = ArrayPool<byte>.Shared.Rent(prepared.EncodedLength);
        CacheEntryCodec.Write(prepared.ObjectEntry, pooledBuffer);
        return prepared.EncodedLength;
    }

    public static bool TryDecode<T>(ReadOnlySpan<byte> source, out CacheEntry<T>? entry)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out _))
            return true;
        entry = null;
        return false;
    }
}
