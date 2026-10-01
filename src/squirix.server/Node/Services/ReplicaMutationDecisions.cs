using System;
using System.Collections.Frozen;
using System.Runtime.InteropServices;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Decides the outcome, the effect and the pinned deadline of each replicated mutation kind from one prepare-time read.</summary>
/// <remarks>
/// One function per kind: it takes the live entry the leader observed and the prepare time, and returns everything the record
/// carries about the decision. Nothing after prepare recomputes any of it.
/// </remarks>
internal static class ReplicaMutationDecisions
{
    /// <summary>Decides an unconditional write: applied, writing the requested entry under its resolved deadline.</summary>
    /// <param name="entry">The requested entry.</param>
    /// <param name="now">The prepare time.</param>
    /// <returns>The decision.</returns>
    internal static ReplicaDecision DecideSet(NodeCacheEntry<object?> entry, DateTime now) => Upsert(entry.Value, entry.Version, entry.Tags, DeadlineTicks(entry, now));

    /// <summary>Decides a conditional add: applied, writing the requested entry, when the key is absent; otherwise nothing changes.</summary>
    /// <param name="current">The live entry the leader observed, or <see langword="null" /> when the key is absent.</param>
    /// <param name="entry">The requested entry.</param>
    /// <param name="now">The prepare time.</param>
    /// <returns>The decision.</returns>
    internal static ReplicaDecision DecideTryAdd(NodeCacheEntry<object?>? current, NodeCacheEntry<object?> entry, DateTime now) =>
        current == null ? DecideSet(entry, now) : Unchanged();

    /// <summary>Decides a value replacement: applied, writing the new value over the observed entry, when the key is live; otherwise nothing changes.</summary>
    /// <param name="current">The live entry the leader observed, or <see langword="null" /> when the key is absent.</param>
    /// <param name="value">The replacement value.</param>
    /// <returns>The decision, which keeps the observed version, tags and deadline.</returns>
    internal static ReplicaDecision DecideUpdate(NodeCacheEntry<object?>? current, object? value) =>
        current == null ? Unchanged() : Upsert(value, current.Version, current.Tags, PinnedTicks(current.ExpiresUtc));

    /// <summary>Decides an expiration refresh: applied, writing the observed entry under the new deadline, when the key is live; otherwise nothing changes.</summary>
    /// <param name="current">The live entry the leader observed, or <see langword="null" /> when the key is absent.</param>
    /// <param name="now">The prepare time.</param>
    /// <param name="expiration">The new expiration, measured from <paramref name="now" />.</param>
    /// <returns>The decision.</returns>
    internal static ReplicaDecision DecideTouch(NodeCacheEntry<object?>? current, DateTime now, TimeSpan expiration) =>
        current == null ? Unchanged() : Upsert(current.Value, current.Version, current.Tags, PinnedTicks(ExpiresAt(now, expiration)));

    /// <summary>Decides an expiration removal: applied, writing the observed entry without a deadline, when the live entry has one; otherwise nothing changes.</summary>
    /// <param name="current">The live entry the leader observed, or <see langword="null" /> when the key is absent.</param>
    /// <returns>The decision.</returns>
    internal static ReplicaDecision DecideRemoveExpiration(NodeCacheEntry<object?>? current) =>
        current?.ExpiresUtc == null ? Unchanged() : Upsert(current.Value, current.Version, current.Tags, 0);

    /// <summary>Decides a remove: applied when the key is live, with the removed entry as the outcome; the effect deletes the key either way.</summary>
    /// <param name="current">The live entry the leader observed, or <see langword="null" /> when the key is absent.</param>
    /// <returns>The decision.</returns>
    internal static ReplicaDecision DecideRemove(NodeCacheEntry<object?>? current) => new(
        [],
        current == null ? ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty) : ReplicaOutcomeCodec.Encode(true, current.MapToProto().ToByteArray()),
        0);

    /// <summary>Returns the effective absolute deadline of an entry written at <paramref name="now" />.</summary>
    /// <param name="entry">The entry to write.</param>
    /// <param name="now">The prepare time.</param>
    /// <returns>
    /// The UTC ticks of the earlier of the entry's absolute expiration and its relative expiration measured from
    /// <paramref name="now" />, at least one; zero when the entry never expires.
    /// </returns>
    /// <remarks>
    /// The relative expiration is resolved here, once, so a replay of the record keeps the deadline of the original write
    /// instead of measuring the relative expiration again from the replay time.
    /// </remarks>
    private static long DeadlineTicks(NodeCacheEntry<object?> entry, DateTime now)
    {
        var deadline = entry.ExpiresUtc;
        if (entry.Expiration is { } expiration && (deadline == null || ExpiresAt(now, expiration) < deadline))
            deadline = ExpiresAt(now, expiration);

        return PinnedTicks(deadline);
    }

    private static DateTime ExpiresAt(DateTime now, TimeSpan expiration) => now.SaturatedAdd(expiration);

    /// <summary>Pins a deadline to whole milliseconds, the precision the journal and the snapshots store, so every copy of the entry agrees.</summary>
    /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
    /// <returns>The UTC ticks of the pinned deadline; zero for none.</returns>
    private static long PinnedTicks(DateTime? expiresUtc) => JournalEntryExpirationMaterializer.PinToJournalPrecision(expiresUtc)?.Ticks ?? 0;

    private static ReplicaDecision Unchanged() => new([], ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty), 0);

    /// <summary>Decides an upsert of the entry, refusing one the apply would refuse.</summary>
    /// <param name="value">The value.</param>
    /// <param name="version">The entry version.</param>
    /// <param name="tags">The entry tags.</param>
    /// <param name="expiresUtcTicks">The pinned deadline in UTC ticks, or zero.</param>
    /// <returns>The decision.</returns>
    /// <exception cref="Errors.SquirixException">The entry, with its deadline, exceeds the entry size or tag limits.</exception>
    /// <remarks>
    /// The apply writes the entry with its deadline through the cache journal, which refuses an oversized entry. Refused there, after the
    /// majority, the entry would stay pending and block the group; refused here, nothing is appended and the caller gets the limit error.
    /// </remarks>
    private static ReplicaDecision Upsert(object? value, long version, FrozenDictionary<string, string>? tags, long expiresUtcTicks)
    {
        DateTime? expiresUtc = expiresUtcTicks == 0 ? null : new DateTime(expiresUtcTicks, DateTimeKind.Utc);
        JournalEntryPayload.EnsureEncodedLengthWithinLimit(new NodeCacheEntry<object?>(value, version, expiresUtc, null, tags));
        return new ReplicaDecision(
            ReplicaCacheApplier.EncodeEntry(new NodeCacheEntry<object?>(value, version, null, null, tags)),
            ReplicaOutcomeCodec.Encode(true, ReadOnlyMemory<byte>.Empty),
            expiresUtcTicks);
    }

    /// <summary>The outcome, effect payload and pinned deadline the leader decided for one mutation.</summary>
    /// <param name="Payload">The entry an upserting record writes, or empty when the effect is a delete or nothing.</param>
    /// <param name="Outcome">The canonical outcome the client, the log and the idempotency state share.</param>
    /// <param name="ExpiresUtcTicks">The pinned absolute deadline of the written entry, or zero.</param>
    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct ReplicaDecision(byte[] Payload, byte[] Outcome, long ExpiresUtcTicks);
}
