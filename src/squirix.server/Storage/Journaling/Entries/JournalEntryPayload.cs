using System;
using Squirix.Server.Storage.Entries.Binary;

namespace Squirix.Server.Storage.Journaling.Entries;

/// <summary>Sync encode/decode of journal Put payloads via <see cref="CacheEntryCodec" />.</summary>
internal static class JournalEntryPayload
{
    public static byte[] Encode<T>(CacheEntry<T> entry)
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        var objectEntry = new CacheEntry<object?>
        {
            Value = entry.Value,
            ExpiresUtc = expiresUtc,
            Expiration = expiration,
            Version = entry.Version,
            Tags = entry.Tags,
        };
        var length = CacheEntryCodec.ComputeEncodedLength(objectEntry);
        var buffer = new byte[length];
        CacheEntryCodec.Write(objectEntry, buffer);
        return buffer;
    }

    public static bool TryDecode<T>(ReadOnlySpan<byte> source, out CacheEntry<T>? entry)
    {
        if (CacheEntryCodec.TryRead(source, out entry, out _))
            return true;
        entry = null;
        return false;
    }
}
