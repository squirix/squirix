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
            return PinAbsoluteOnly(entry);

        var relativeDeadline = utcNow.SaturatedAdd(relative);
        var effective = entry.ExpiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return new NodeCacheEntry<T>(entry.Value, entry.Version, PinToJournalPrecision(effective), tags: entry.Tags);
    }

    /// <summary>
    /// Pins a deadline to whole milliseconds, the precision the journal and the snapshots store, so every copy of the entry agrees.
    /// It rounds up, so an entry never lives shorter than it was asked to.
    /// </summary>
    /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
    /// <returns>
    /// The deadline rounded up to a whole millisecond and clamped to the largest whole millisecond a date can hold, and at least one
    /// millisecond of ticks; <see langword="null" /> for none.
    /// </returns>
    internal static DateTime? PinToJournalPrecision(DateTime? expiresUtc)
    {
        if (expiresUtc is not { } deadline)
            return null;

        var ticks = deadline.Ticks;
        var remainder = ticks % TimeSpan.TicksPerMillisecond;
        var rounded = remainder == 0 ? ticks : ticks + (TimeSpan.TicksPerMillisecond - remainder);
        var largest = DateTime.MaxValue.Ticks - (DateTime.MaxValue.Ticks % TimeSpan.TicksPerMillisecond);
        return new DateTime(Math.Min(Math.Max(rounded, TimeSpan.TicksPerMillisecond), largest), DateTimeKind.Utc);
    }

    /// <summary>Returns the deadline a journal frame stores; a relative expiration must already be resolved against the server clock.</summary>
    /// <param name="expiresUtc">The absolute deadline, or <see langword="null" /> for none.</param>
    /// <param name="expiration">The relative expiration, which must be <see langword="null" />.</param>
    /// <returns>The absolute deadline the frame stores.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="expiration" /> is set: resolve it with <see cref="ForDurableWrite{T}" /> first.</exception>
    internal static DateTime? ForJournalWrite(DateTime? expiresUtc, TimeSpan? expiration) =>
        expiration == null
            ? expiresUtc
            : throw new InvalidOperationException("A relative expiration must be resolved to an absolute deadline on the server clock before it is journaled.");

    /// <summary>Returns a deadline of the same encoded length as the one the journal frame of this entry will store.</summary>
    /// <param name="expiresUtc">The absolute deadline, or <see langword="null" /> for none.</param>
    /// <param name="expiration">The relative expiration, or <see langword="null" /> for none.</param>
    /// <returns>
    /// <paramref name="expiresUtc" />, or a placeholder when only a relative expiration is set: the durable write resolves it to one absolute
    /// deadline, whose encoded length does not depend on its value.
    /// </returns>
    internal static DateTime? ForJournalSizing(DateTime? expiresUtc, TimeSpan? expiration) => expiresUtc ?? (expiration == null ? null : DateTime.MaxValue);

    internal static NodeCacheEntry<T> ForRecoveryInsert<T>(NodeCacheEntry<T> entry, long writtenUnixMs)
    {
        if (entry.Expiration is not { } relative || writtenUnixMs <= 0)
            return entry;

        var time = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        var relativeDeadline = time.SaturatedAdd(relative);
        var effective = entry.ExpiresUtc is { } absolute && absolute < relativeDeadline ? absolute : relativeDeadline;
        return new NodeCacheEntry<T>(entry.Value, entry.Version, effective, tags: entry.Tags);
    }

    /// <summary>Decides whether a journaled entry is expired at <paramref name="utcNow" />, the server clock of the node replaying it.</summary>
    /// <param name="expiresUtc">The absolute deadline, or <see langword="null" /> for none.</param>
    /// <param name="expiration">The relative expiration, or <see langword="null" /> for none.</param>
    /// <param name="writtenUnixMs">The frame write time a relative expiration is measured from, or zero when unknown.</param>
    /// <param name="utcNow">The server clock time of the replay.</param>
    /// <returns><see langword="true" /> when the entry is expired.</returns>
    internal static bool IsExpiredForRecovery(DateTime? expiresUtc, TimeSpan? expiration, long writtenUnixMs, DateTime utcNow)
    {
        if (expiresUtc is { } utc && utc <= utcNow)
            return true;

        if (expiration is not { } relative)
            return false;

        if (relative <= TimeSpan.Zero)
            return true;

        if (writtenUnixMs <= 0)
            return false;

        var writtenAt = DateTimeOffset.FromUnixTimeMilliseconds(writtenUnixMs).UtcDateTime;
        return writtenAt.SaturatedAdd(relative) <= utcNow;
    }

    private static NodeCacheEntry<T> PinAbsoluteOnly<T>(NodeCacheEntry<T> entry)
    {
        var pinned = PinToJournalPrecision(entry.ExpiresUtc);
        return pinned == entry.ExpiresUtc ? entry : new NodeCacheEntry<T>(entry.Value, entry.Version, pinned, tags: entry.Tags);
    }
}
