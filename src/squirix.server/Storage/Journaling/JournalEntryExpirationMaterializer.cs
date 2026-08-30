using System;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Normalizes cache entry expiration for durable journal write and recovery replay.</summary>
internal static class JournalEntryExpirationMaterializer
{
    internal static (DateTime? ExpiresUtc, TimeSpan? Expiration) ForJournalWrite(DateTime? expiresUtc, TimeSpan? expiration)
    {
        if (expiration is not { } relative)
            return (expiresUtc, null);

        var relativeDeadline = DateTime.UtcNow.Add(relative);
        var effective = expiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return (effective, null);
    }

    internal static NodeCacheEntry<T> ForRecoveryInsert<T>(NodeCacheEntry<T> entry, long writtenUnixMs)
    {
        if (entry.Expiration is not { } relative || writtenUnixMs <= 0)
            return entry;

        var time = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        var relativeDeadline = time.Add(relative);
        var effective = entry.ExpiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return new NodeCacheEntry<T>(entry.Value, entry.Version, effective, tags: entry.Tags);
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
}
