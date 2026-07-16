using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Normalizes cache entry expiration for durable journal write and recovery replay.</summary>
internal static class JournalEntryExpirationMaterializer
{
    internal static NodeCacheEntry<T> ForRecoveryInsert<T>(NodeCacheEntry<T> entry, long writtenUnixMs)
    {
        if (entry.ExpiresUtc is not null || entry.Expiration is null || writtenUnixMs <= 0)
            return entry;

        var time = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        return new NodeCacheEntry<T>(entry.Value, entry.Version, time.Add(entry.Expiration.Value), tags: entry.Tags);
    }

    internal static bool IsExpiredForRecovery(DateTime? expiresUtc, TimeSpan? expiration, long writtenUnixMs)
    {
        if (expiresUtc is { } utc && utc <= DateTime.UtcNow)
            return true;

        if (expiration is not { } relative)
            return false;

        if (relative <= TimeSpan.Zero)
            return true;

        if (writtenUnixMs <= 0)
            return false;

        var writtenAt = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        return writtenAt.Add(relative) <= DateTime.UtcNow;
    }

    internal static (DateTime? ExpiresUtc, TimeSpan? Expiration) ForJournalWrite(DateTime? expiresUtc, TimeSpan? expiration)
    {
        if (expiresUtc is not null || expiration is null)
            return (expiresUtc, expiration);

        return (DateTime.UtcNow.Add(expiration.Value), null);
    }
}
