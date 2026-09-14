using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
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
    /// <summary>Applies one replicated record to the local cache.</summary>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="record">Canonical record to apply.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when the mutation took effect; otherwise <see langword="false" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the mutation kind is unknown.</exception>
    internal static async Task<bool> ApplyAsync(ILogicalNamespacedCache<object?> cache, ReplicaLogRecord record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);
        var key = Encoding.UTF8.GetString(record.KeyPayload.Span);
        switch (record.MutationKind)
        {
            case ReplicaMutationKinds.Set:
                var setEntry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(record.MutationPayload));
                var setValue = await setEntry.MapFromProtoAsync<object?>().ConfigureAwait(false);
                await cache.SetEntryAsync(record.OperationId, record.CacheName, key, setValue, cancellationToken).ConfigureAwait(false);
                return true;
            case ReplicaMutationKinds.TryAdd:
                var addEntry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(record.MutationPayload));
                var addValue = await addEntry.MapFromProtoAsync<object?>().ConfigureAwait(false);
                return await cache.TryAddEntryAsync(record.OperationId, record.CacheName, key, addValue, cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Remove:
                return (await cache.RemoveAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false)).Removed;
            case ReplicaMutationKinds.RemoveExpiration:
                return await cache.RemoveExpirationAsync(record.OperationId, record.CacheName, key, cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Touch:
                // The Touch wire value is a TimeSpan duration in ticks, not an absolute UTC timestamp
                // like the other mutation kinds carry in ExpiresUtcTicks.
                return await cache.TouchAsync(record.OperationId, record.CacheName, key, TimeSpan.FromTicks(record.ExpiresUtcTicks), cancellationToken).ConfigureAwait(false);
            case ReplicaMutationKinds.Update:
                var updateValue = CacheValue.Parser.ParseFrom(new ReadOnlySequence<byte>(record.MutationPayload));
                var value = await ServerProtoEx.MapCacheValueAsync<object?>(updateValue).ConfigureAwait(false);
                return await cache.UpdateAsync(record.OperationId, record.CacheName, key, value, cancellationToken).ConfigureAwait(false);
            default:
                throw new InvalidOperationException($"Unsupported replica mutation kind '{record.MutationKind}'.");
        }
    }
}
