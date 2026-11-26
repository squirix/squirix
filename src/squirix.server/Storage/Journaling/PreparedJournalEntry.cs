using System;
using System.Collections.Frozen;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Journal put payload materialized once for sizing and encode.</summary>
[Immutable]
internal abstract class PreparedJournalEntry
{
    private PreparedJournalEntry(int encodedLength)
    {
        EncodedLength = encodedLength;
    }

    internal int EncodedLength { get; }

    /// <summary>Prepares the deadline-free replica payload.</summary>
    /// <param name="value">The requested value.</param>
    /// <param name="version">The entry version.</param>
    /// <param name="tags">The entry tags.</param>
    /// <returns>The measured entry for the mutation payload.</returns>
    internal static PreparedJournalEntry ForReplicaUpsert(in StoredValue value, long version, FrozenDictionary<string, string>? tags)
    {
        EntryTagsGuard.EnsureWithinLimits(tags);
        return From(new NodeCacheEntry<StoredValue>(value, version, tags: tags));
    }

    internal static PreparedJournalEntry From<T>(NodeCacheEntry<T> entry) => Prepare(entry, null);

    /// <summary>Prepares an entry whose journal-encoded length the caller already knows, without measuring the value again.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to prepare; it carries an absolute deadline or none.</param>
    /// <param name="encodedLength">The encoded length of <paramref name="entry" />, as <see cref="From{T}(NodeCacheEntry{T})" /> would measure it.</param>
    /// <returns>The prepared entry.</returns>
    /// <exception cref="InvalidOperationException">The entry has a relative expiration.</exception>
    internal static PreparedJournalEntry From<T>(NodeCacheEntry<T> entry, int encodedLength) => Prepare(entry, encodedLength);

    /// <summary>Measures the encoded length the journal frame of <paramref name="entry" /> will have, before its expiration is resolved.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to measure.</param>
    /// <returns>The encoded length.</returns>
    internal static int MeasureEncodedLength<T>(NodeCacheEntry<T> entry) => CacheEntryCodec.ComputeEncodedLength(
        JournalEntryExpirationMaterializer.ForJournalSizing(entry.ExpiresUtc, entry.Expiration),
        null,
        entry.Tags,
        entry is NodeCacheEntry<StoredValue> stored ? CacheEntryCodec.ComputeValueLength(stored.RawValue) : CacheEntryCodec.ComputeValueLength(entry.Normalize()));

    internal abstract int Measure();

    internal abstract int WriteTo(Span<byte> destination);

    private static PreparedJournalEntry Prepare<T>(NodeCacheEntry<T> entry, int? encodedLength)
    {
        var expires = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        if (entry is NodeCacheEntry<StoredValue> stored)
            return new Payload<StoredValue>(stored, encodedLength);

        var normalized = entry.Normalize();
        var durable = entry is NodeCacheEntry<object?> original && ReferenceEquals(normalized, original.RawValue)
            ? original
            : new NodeCacheEntry<object?>(normalized, entry.Version, expires, null, entry.Tags);
        return new Payload<object?>(durable, encodedLength);
    }

    private sealed class Payload<T> : PreparedJournalEntry
    {
        private readonly NodeCacheEntry<T> _entry;

        internal Payload(NodeCacheEntry<T> entry, int? encodedLength)
            : base(encodedLength ?? CacheEntryCodec.ComputeEncodedLength(entry))
        {
            _entry = entry;
        }

        internal override int Measure() => CacheEntryCodec.ComputeEncodedLength(_entry);

        internal override int WriteTo(Span<byte> destination) => CacheEntryCodec.WriteMeasured(_entry, destination);
    }
}
