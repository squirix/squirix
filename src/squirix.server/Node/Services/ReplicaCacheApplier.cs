using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Applies replicated records to a local cache pipeline.</summary>
/// <remarks>
/// The owner applies committed mutations through its pipeline; the same entry point will serve
/// follower-side application once commit propagation lands. Malformed records fail fast with an
/// exception rather than diverging silently from the committed outcome.
/// </remarks>
internal static class ReplicaCacheApplier
{
    /// <summary>The shortest expiration a Touch applies, so a deadline that already passed expires the entry instead of being skipped.</summary>
    private static readonly TimeSpan MinTouchExpiration = TimeSpan.FromTicks(1);

    /// <summary>Applies one replicated record to the local cache.</summary>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="record">Canonical record to apply.</param>
    /// <param name="clock">Time source measuring what is left of a Touch deadline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when the mutation took effect; otherwise <see langword="false" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the mutation kind is unknown or a Touch record carries no deadline.</exception>
    /// <remarks>
    /// The absolute deadline pinned in <see cref="ReplicaLogRecord.ExpiresUtcTicks" /> is authoritative: a Set or TryAdd entry takes
    /// it in place of the expirations of its payload, so applying the same record again, at any time, sets the same deadline.
    /// </remarks>
    internal static async Task<bool> ApplyAsync(ILogicalNamespacedCache<object?> cache, ReplicaLogRecord record, TimeProvider clock, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(clock);
        var key = Encoding.UTF8.GetString(record.KeyPayload.Span);
        switch (record.MutationKind)
        {
            case ReplicaMutationKinds.Set:
                var setEntry = await PinnedEntryAsync(record).ConfigureAwait(false);
                await cache.SetEntryAsync(record.OperationId, record.CacheName, key, setEntry, cancellationToken).ConfigureAwait(false);
                return true;
            case ReplicaMutationKinds.TryAdd:
                var addEntry = await PinnedEntryAsync(record).ConfigureAwait(false);
                return await cache.TryAddEntryAsync(record.OperationId, record.CacheName, key, addEntry, cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Remove:
                return (await cache.RemoveAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false)).Removed;
            case ReplicaMutationKinds.RemoveExpiration:
                return await cache.RemoveExpirationAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Touch:
                return await cache.TouchAsync(record.OperationId, record.CacheName, key, RemainingUntil(in record, clock), cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Update:
                var updateValue = CacheValue.Parser.ParseFrom(new ReadOnlySequence<byte>(record.MutationPayload));
                var value = await ServerProtoEx.MapCacheValueAsync<object?>(updateValue).ConfigureAwait(false);
                return await cache.UpdateAsync(record.OperationId, record.CacheName, key, value, cancellationToken).ConfigureAwait(false);
            default:
                throw new InvalidOperationException($"Unsupported replica mutation kind '{record.MutationKind}'.");
        }
    }

    /// <summary>Decodes the entry of a Set or TryAdd record with the record's pinned deadline as its only expiration.</summary>
    /// <param name="record">The Set or TryAdd record.</param>
    /// <returns>The entry to write.</returns>
    private static async Task<NodeCacheEntry<object?>> PinnedEntryAsync(ReplicaLogRecord record)
    {
        var wire = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(record.MutationPayload));
        var decoded = await wire.MapFromProtoAsync<object?>().ConfigureAwait(false);
        DateTime? expiresUtc = record.ExpiresUtcTicks == 0 ? null : new DateTime(record.ExpiresUtcTicks, DateTimeKind.Utc);
        return new NodeCacheEntry<object?> { Value = decoded.Value, Version = decoded.Version, ExpiresUtc = expiresUtc };
    }

    /// <summary>Returns what is left of the pinned Touch deadline, at least one tick.</summary>
    /// <param name="record">The Touch record.</param>
    /// <param name="clock">Time source of the apply.</param>
    /// <returns>The expiration to touch the entry with.</returns>
    /// <exception cref="InvalidOperationException">The record carries no deadline.</exception>
    /// <remarks>
    /// A deadline that already passed, as in a replay long after the write, is clamped to one tick instead of being skipped: the
    /// entry then expires, as it would have without the replay, and the cache pipeline never sees a non-positive expiration.
    /// </remarks>
    private static TimeSpan RemainingUntil(in ReplicaLogRecord record, TimeProvider clock)
    {
        if (record.ExpiresUtcTicks <= 0)
            throw new InvalidOperationException($"Replica Touch record {record.LogIndex} carries no expiration deadline.");

        var remaining = new DateTime(record.ExpiresUtcTicks, DateTimeKind.Utc) - clock.GetUtcNow().UtcDateTime;
        return remaining < MinTouchExpiration ? MinTouchExpiration : remaining;
    }
}
