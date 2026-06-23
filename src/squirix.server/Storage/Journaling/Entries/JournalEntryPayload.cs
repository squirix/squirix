using System;
using System.Buffers;
using Squirix.Server.Storage.Entries.Binary;

namespace Squirix.Server.Storage.Journaling.Entries;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    public static int ComputeEncodedLength<T>(CacheEntry<T> entry) => CacheEntryCodec.ComputeEncodedLength(ToObjectEntry(entry));

    public static byte[] Encode<T>(CacheEntry<T> entry)
    {
        var objectEntry = ToObjectEntry(entry);
        var buffer = new byte[CacheEntryCodec.ComputeEncodedLength(objectEntry)];
        CacheEntryCodec.Write(objectEntry, buffer);
        return buffer;
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
    public static int Encode<T>(CacheEntry<T> entry, out byte[] pooledBuffer)
    {
        var objectEntry = ToObjectEntry(entry);
        var length = CacheEntryCodec.ComputeEncodedLength(objectEntry);
        pooledBuffer = ArrayPool<byte>.Shared.Rent(length);
        CacheEntryCodec.Write(objectEntry, pooledBuffer);
        return length;
    }

    public static bool TryDecode<T>(ReadOnlySpan<byte> source, out CacheEntry<T>? entry)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out _))
            return true;
        entry = null;
        return false;
    }

    private static CacheEntry<object?> ToObjectEntry<T>(CacheEntry<T> entry)
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        return new CacheEntry<object?>
        {
            Value = CacheEntryCodec.NormalizeValue(entry.Value),
            ExpiresUtc = expiresUtc,
            Expiration = expiration,
            Version = entry.Version,
            Tags = entry.Tags,
        };
    }
}
