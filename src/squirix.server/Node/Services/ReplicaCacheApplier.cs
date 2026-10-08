using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the effect a replicated record carries to a local cache pipeline.</summary>
/// <remarks>
/// The leader decides the outcome and the effect of a mutation once, at prepare time, and the record carries both. Applying a
/// record never reads a clock and never re-checks liveness or preconditions: it writes the exact entry, removes the key, or leaves
/// memory alone, so applying the same record at any time on any node yields the same entry. A record whose effect disagrees with
/// its outcome is refused before memory is touched.
/// </remarks>
internal static class ReplicaCacheApplier
{
    /// <summary>Applies one replicated record to the local cache.</summary>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="record">Canonical record to apply.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the effect is applied.</returns>
    /// <exception cref="InvalidDataException">The record is inconsistent; memory is left untouched.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The effect of the record is not supported.</exception>
    /// <remarks>
    /// The absolute deadline pinned in <see cref="ReplicaLogRecord.ExpiresUtcTicks" /> is authoritative for an upserted entry, so
    /// applying the same record again, at any time, writes the same deadline.
    /// </remarks>
    internal static Task ApplyAsync(ILogicalNamespacedCache<object?> cache, in ReplicaLogRecord record, CancellationToken cancellationToken)
    {
        var effect = Resolve(in record, out var entry);
        return ExecuteAsync(cache, record, effect, entry, cancellationToken);
    }

    /// <summary>Executes the effect of a record that <see cref="Resolve" /> accepted.</summary>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="record">The validated record.</param>
    /// <param name="effect">The effect <see cref="Resolve" /> returned.</param>
    /// <param name="entry">The decoded entry <see cref="Resolve" /> returned for an upsert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the effect is applied.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The effect is not supported.</exception>
    /// <exception cref="ArgumentNullException">An upsert has no entry.</exception>
    internal static async Task ExecuteAsync(
        ILogicalNamespacedCache<object?> cache,
        ReplicaLogRecord record,
        ReplicaEffectKind effect,
        NodeCacheEntry<object?>? entry,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);
        var key = Encoding.UTF8.GetString(record.KeyPayload.Span);
        switch (effect)
        {
            case ReplicaEffectKind.Upsert:
                ArgumentNullException.ThrowIfNull(entry);
                await cache.SetEntryAsync(record.OperationId, record.CacheName, key, entry, cancellationToken).ConfigureAwait(false);
                break;
            case ReplicaEffectKind.Delete:
                // The result reflects the local liveness of the key, not the committed outcome.
                _ = await cache.RemoveAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false);
                break;
            case ReplicaEffectKind.Unchanged or ReplicaEffectKind.NoCacheEffect:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(record), effect, "Unsupported replica effect.");
        }
    }

    /// <summary>Encodes an entry for the mutation payload of an upserting record.</summary>
    /// <param name="entry">The entry the record writes; its deadline travels in the record, not in the entry.</param>
    /// <returns>The durable entry encoding, which keeps the version and the tags.</returns>
    internal static byte[] EncodeEntry(NodeCacheEntry<object?> entry)
    {
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var pooled = JournalEntryPayload.Encode(in prepared);
        return pooled.Memory.ToArray();
    }

    /// <summary>Determines the effect of a record and checks that it agrees with the outcome of the record.</summary>
    /// <param name="record">The record to check.</param>
    /// <returns>The effect applying the record has.</returns>
    /// <exception cref="InvalidDataException">The mutation kind is unknown, the outcome is undecodable, or the effect fields contradict the outcome.</exception>
    /// <remarks>
    /// Set upserts and is always applied. TryAdd, Update, Touch and RemoveExpiration upsert the resulting entry when applied and change
    /// nothing otherwise, with an empty payload and no deadline. Remove deletes the key whatever the outcome, carries no payload, and
    /// reports the removed entry only when applied. Expire deletes the key the leader found expired: not applied, no payload, and the
    /// passed deadline. Update, Touch and RemoveExpiration that found the key expired carry the same shape and delete it too. A leader-term
    /// no-op touches no cache: not applied, under the leader-term scope, with no cache, key, payload or deadline.
    /// </remarks>
    internal static ReplicaEffectKind ResolveEffect(in ReplicaLogRecord record) => Resolve(in record, out _);

    /// <summary>Determines the effect of a record, checks it against the outcome, and decodes the entry an upsert writes.</summary>
    /// <param name="record">The record to check.</param>
    /// <param name="entry">The entry to write, with the pinned deadline, for an upsert; otherwise <see langword="null" />.</param>
    /// <returns>The effect applying the record has.</returns>
    /// <exception cref="InvalidDataException">The record is inconsistent, out of range, or its entry does not decode.</exception>
    internal static ReplicaEffectKind Resolve(in ReplicaLogRecord record, out NodeCacheEntry<object?>? entry)
    {
        var effect = ResolveShape(in record);
        entry = effect == ReplicaEffectKind.Upsert ? DecodeEntry(in record) : null;
        return effect;
    }

    /// <summary>Determines the effect of a record and checks its fields against its outcome, without decoding the entry an upsert writes.</summary>
    /// <param name="record">The record to check.</param>
    /// <returns>The effect applying the record has.</returns>
    /// <exception cref="InvalidDataException">The record is inconsistent or out of range.</exception>
    internal static ReplicaEffectKind ResolveShape(in ReplicaLogRecord record)
    {
        if (!ReplicaOutcomeCodec.TryDecode(record.OutcomePayload, out var applied, out var previous))
            throw Inconsistent(in record, "the outcome payload is undecodable", false);

        var deadlineInRange = record.ExpiresUtcTicks >= 0 && record.ExpiresUtcTicks <= DateTime.MaxValue.Ticks;
        return !deadlineInRange ? throw Inconsistent(in record, "the deadline is out of range", applied) : record.MutationKind switch
        {
            ReplicaMutationKinds.LeaderNoop => ResolveLeaderNoop(in record, applied, previous),
            ReplicaMutationKinds.Remove => ResolveRemove(in record, applied, previous),
            ReplicaMutationKinds.Expire => ResolveExpire(in record, applied, previous),

            // A conditional mutation that found the key expired folds the expiry into its record and deletes the key; Set and TryAdd write
            // over an expired key instead, so they never carry this shape.
            ReplicaMutationKinds.Update or ReplicaMutationKinds.Touch or ReplicaMutationKinds.RemoveExpiration when IsExpiryShape(in record, applied, previous) =>
                ReplicaEffectKind.Delete,
            _ => ResolveConditional(in record, applied, previous),
        };
    }

    /// <summary>Tells whether a record has the shape of a deletion of an expired key: not applied, no payload, a deadline, no previous entry.</summary>
    /// <param name="record">The record.</param>
    /// <param name="applied">The applied flag of its outcome.</param>
    /// <param name="previous">The previous entry its outcome reports.</param>
    /// <returns><see langword="true" /> for the expiry shape.</returns>
    private static bool IsExpiryShape(in ReplicaLogRecord record, bool applied, ReadOnlyMemory<byte> previous) =>
        !applied && record.MutationPayload.IsEmpty && record.ExpiresUtcTicks != 0 && previous.IsEmpty;

    private static ReplicaEffectKind ResolveExpire(in ReplicaLogRecord record, bool applied, ReadOnlyMemory<byte> previous) =>
        IsExpiryShape(in record, applied, previous) ? ReplicaEffectKind.Delete
            : throw Inconsistent(in record, "an expiration is applied, carries a payload or a previous entry, or has no deadline", applied);

    private static ReplicaEffectKind ResolveLeaderNoop(in ReplicaLogRecord record, bool applied, ReadOnlyMemory<byte> previous)
    {
        var wellFormed = !applied && previous.IsEmpty && record.MutationPayload.IsEmpty && record.ExpiresUtcTicks == 0 && record.KeyPayload.IsEmpty &&
            record.CacheName.Length == 0 && string.Equals(record.OperationScope, ReplicaLeaderOperationId.OperationScope, StringComparison.Ordinal);
        return wellFormed ? ReplicaEffectKind.NoCacheEffect
            : throw Inconsistent(in record, "a leader-term no-op is applied, names a cache, a key or a deadline, carries a payload, or has another scope", applied);
    }

    private static ReplicaEffectKind ResolveRemove(in ReplicaLogRecord record, bool applied, ReadOnlyMemory<byte> previous)
    {
        var wellFormed = record.MutationPayload.IsEmpty && record.ExpiresUtcTicks == 0 && applied == !previous.IsEmpty;
        return wellFormed ? ReplicaEffectKind.Delete : throw Inconsistent(in record, "a remove carries a payload or a deadline, or its removed entry disagrees with the applied flag", applied);
    }

    private static ReplicaEffectKind ResolveConditional(in ReplicaLogRecord record, bool applied, ReadOnlyMemory<byte> previous)
    {
        var hasDeadline = record.ExpiresUtcTicks != 0;
        var hasPayload = !record.MutationPayload.IsEmpty;
        var deadlineFits = record.MutationKind switch
        {
            ReplicaMutationKinds.Set or ReplicaMutationKinds.TryAdd or ReplicaMutationKinds.Update => true,
            ReplicaMutationKinds.Touch => hasDeadline || !applied,
            ReplicaMutationKinds.RemoveExpiration => !hasDeadline,
            _ => throw Inconsistent(in record, "the mutation kind is unknown", applied),
        };

        var appliedFits = applied || !string.Equals(record.MutationKind, ReplicaMutationKinds.Set, StringComparison.Ordinal);
        var upserts = applied && hasPayload;
        var unchanged = !applied && !hasPayload && !hasDeadline;
        var effect = applied ? ReplicaEffectKind.Upsert : ReplicaEffectKind.Unchanged;
        return deadlineFits && appliedFits && previous.IsEmpty && (upserts || unchanged)
            ? effect
            : throw Inconsistent(in record, "the effect contradicts the outcome: the payload, the deadline or the previous entry does not fit the mutation kind and the applied flag", applied);
    }

    private static NodeCacheEntry<object?> DecodeEntry(in ReplicaLogRecord record)
    {
        if (!JournalEntryPayload.TryDecode<object?>(record.MutationPayload.Span, out var decoded) || decoded == null)
            throw Inconsistent(in record, "the entry payload is undecodable", true);

        DateTime? expiresUtc = record.ExpiresUtcTicks == 0 ? null : new DateTime(record.ExpiresUtcTicks, DateTimeKind.Utc);
        return new NodeCacheEntry<object?>(decoded.Value, decoded.Version, expiresUtc, null, decoded.Tags);
    }

    private static InvalidDataException Inconsistent(in ReplicaLogRecord record, string reason, bool applied) => new(
        $"Replica log entry {record.LogIndex} ({record.MutationKind}, applied flag {applied}) is inconsistent: {reason}.");
}
