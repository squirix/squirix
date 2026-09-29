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
    internal static async Task ApplyAsync(ILogicalNamespacedCache<object?> cache, ReplicaLogRecord record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);
        var effect = ResolveEffect(in record);
        var key = Encoding.UTF8.GetString(record.KeyPayload.Span);
        switch (effect)
        {
            case ReplicaEffectKind.Upsert:
                var entry = DecodeEntry(in record);
                await cache.SetEntryAsync(record.OperationId, record.CacheName, key, entry, cancellationToken).ConfigureAwait(false);
                break;
            case ReplicaEffectKind.Delete:
                // The result reflects the local liveness of the key, not the committed outcome.
                _ = await cache.RemoveAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false);
                break;
            case ReplicaEffectKind.Unchanged:
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
    /// reports the removed entry only when applied.
    /// </remarks>
    internal static ReplicaEffectKind ResolveEffect(in ReplicaLogRecord record)
    {
        if (!ReplicaOutcomeCodec.TryDecode(record.OutcomePayload, out var applied, out var previous))
            throw Inconsistent(in record, "the outcome payload is undecodable", false);

        if (record.ExpiresUtcTicks < 0)
            throw Inconsistent(in record, "the deadline is negative", applied);

        var isRemove = string.Equals(record.MutationKind, ReplicaMutationKinds.Remove, StringComparison.Ordinal);
        return isRemove ? ResolveRemove(in record, applied, previous) : ResolveConditional(in record, applied, previous);
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
