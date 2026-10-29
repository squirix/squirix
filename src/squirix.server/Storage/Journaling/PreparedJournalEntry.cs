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

    /// <summary>Prepares an entry whose journal-encoded length the caller already knows, without measuring the value again.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to prepare; it carries an absolute deadline or none.</param>
    /// <param name="encodedLength">The encoded length of <paramref name="entry" />, as <see cref="From{T}(NodeCacheEntry{T})" /> would measure it.</param>
    /// <returns>The prepared entry.</returns>
    /// <exception cref="InvalidOperationException">The entry has a relative expiration.</exception>
    internal static PreparedJournalEntry From<T>(NodeCacheEntry<T> entry, int encodedLength) =>
        new(ToObjectEntry(entry, JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration)), encodedLength);

    /// <summary>Measures the encoded length the journal frame of <paramref name="entry" /> will have, before its expiration is resolved.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to measure.</param>
    /// <returns>The encoded length.</returns>
    internal static int MeasureEncodedLength<T>(NodeCacheEntry<T> entry) =>
        CacheEntryCodec.ComputeEncodedLength(
            JournalEntryExpirationMaterializer.ForJournalSizing(entry.ExpiresUtc, entry.Expiration), null, entry.Tags, CacheEntryCodec.ComputeValueLength(entry.Normalize()));

    private static NodeCacheEntry<object?> ToObjectEntry<T>(NodeCacheEntry<T> entry, DateTime? expiresUtc)
    {
        var value = entry.Normalize();
        return entry is NodeCacheEntry<object?> objectEntry && entry.Expiration == null && entry.ExpiresUtc == expiresUtc && ReferenceEquals(value, objectEntry.Value)
            ? objectEntry
            : new NodeCacheEntry<object?>(value, entry.Version, expiresUtc, null, entry.Tags);
    }
}
