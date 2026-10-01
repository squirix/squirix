using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Journal put payload materialized once for sizing and encode.</summary>
[Immutable]
internal sealed record PreparedJournalEntry
{
    private PreparedJournalEntry(NodeCacheEntry<object?> objectEntry, int encodedLength)
    {
        ObjectEntry = objectEntry;
        EncodedLength = encodedLength;
    }

    internal int EncodedLength { get; }

    internal NodeCacheEntry<object?> ObjectEntry { get; }

    internal static PreparedJournalEntry From<T>(NodeCacheEntry<T> entry)
    {
        var objectEntry = ToObjectEntry(entry, JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration));
        return new PreparedJournalEntry(objectEntry, CacheEntryCodec.ComputeEncodedLength(objectEntry));
    }

    /// <summary>Measures the encoded length the journal frame of <paramref name="entry" /> will have, before its expiration is resolved.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to measure.</param>
    /// <returns>The encoded length.</returns>
    internal static int MeasureEncodedLength<T>(NodeCacheEntry<T> entry) =>
        CacheEntryCodec.ComputeEncodedLength(ToObjectEntry(entry, JournalEntryExpirationMaterializer.ForJournalSizing(entry.ExpiresUtc, entry.Expiration)));

    private static NodeCacheEntry<object?> ToObjectEntry<T>(NodeCacheEntry<T> entry, DateTime? expiresUtc) =>
        new(entry.Normalize(), entry.Version, expiresUtc, null, entry.Tags);
}
