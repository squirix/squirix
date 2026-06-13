namespace Squirix.Server;

/// <summary>
/// Applies the default replacement rules for entry-based update factories.
/// </summary>
internal static class CacheEntryUpdatePolicy
{
    internal static CacheEntry<T> PreserveExpirationWhenNotSpecified<T>(CacheEntry<T> replacement, CacheEntry<T> existing)
    {
        return replacement.ExpiresUtc is not null || replacement.Expiration is not null ? replacement : new CacheEntry<T>
        {
            Value = replacement.Value,
            ExpiresUtc = existing.ExpiresUtc,
            Expiration = null,
            Version = replacement.Version,
            Tags = replacement.Tags ?? existing.Tags,
        };
    }
}
