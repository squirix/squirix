using Squirix.Server.Storage.Entries.Binary;

namespace Squirix.Server.Storage.Journaling.Entries;

/// <summary>Journal put payload materialized once for sizing and encode.</summary>
internal readonly struct PreparedJournalEntry
{
    private PreparedJournalEntry(CacheEntry<object?> objectEntry, int encodedLength)
    {
        ObjectEntry = objectEntry;
        EncodedLength = encodedLength;
    }

    public CacheEntry<object?> ObjectEntry { get; }

    public int EncodedLength { get; }

    public static PreparedJournalEntry From<T>(CacheEntry<T> entry)
    {
        var objectEntry = ToObjectEntry(entry);
        return new PreparedJournalEntry(objectEntry, CacheEntryCodec.ComputeEncodedLength(objectEntry));
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
