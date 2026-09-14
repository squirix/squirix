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
        var objectEntry = ToObjectEntry(entry);
        return new PreparedJournalEntry(objectEntry, CacheEntryCodec.ComputeEncodedLength(objectEntry));
    }

    private static NodeCacheEntry<object?> ToObjectEntry<T>(NodeCacheEntry<T> entry)
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        return new NodeCacheEntry<object?>(entry.Normalize(), entry.Version, expiresUtc, expiration, entry.Tags);
    }
}
