using System;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Normalizes cache entry expiration for durable journal write and recovery replay.</summary>
internal static class JournalEntryExpirationMaterializer
{
    /// <summary>
    /// Resolves a relative expiration against <paramref name="utcNow" /> into one absolute deadline, so the journal
    /// frame and the in-memory apply of the same mutation share it instead of each reading its own clock.
    /// </summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to write.</param>
    /// <param name="utcNow">The write instant the relative expiration is measured from.</param>
    /// <returns>The entry itself when it has no relative expiration; otherwise a copy with only the absolute deadline.</returns>
    internal static NodeCacheEntry<T> ForDurableWrite<T>(NodeCacheEntry<T> entry, DateTime utcNow)
    {
        if (entry.Expiration is not { } relative)
            return entry;

        var relativeDeadline = utcNow.SaturatedAdd(relative);
        var effective = entry.ExpiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return new NodeCacheEntry<T>(entry.Value, entry.Version, effective, tags: entry.Tags);
    }

    internal static (DateTime? ExpiresUtc, TimeSpan? Expiration) ForJournalWrite(DateTime? expiresUtc, TimeSpan? expiration)
    {
        if (expiration is not { } relative)
            return (expiresUtc, null);

        var relativeDeadline = DateTime.UtcNow.SaturatedAdd(relative);
        var effective = expiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return (effective, null);
    }

    internal static NodeCacheEntry<T> ForRecoveryInsert<T>(NodeCacheEntry<T> entry, long writtenUnixMs)
    {
        if (entry.Expiration is not { } relative || writtenUnixMs <= 0)
            return entry;

        var time = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        var relativeDeadline = time.SaturatedAdd(relative);
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
        return writtenAt.SaturatedAdd(relative) <= DateTime.UtcNow;
    }
}
