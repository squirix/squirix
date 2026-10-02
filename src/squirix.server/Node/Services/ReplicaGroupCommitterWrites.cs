using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;

namespace Squirix.Server.Node.Services;

/// <summary>The typed replicated writes of <see cref="ReplicaGroupCommitter" />: each prepares its own mutation and decodes its own outcome.</summary>
internal static class ReplicaGroupCommitterWrites
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Commits a replicated remove and returns the removed entry, if any.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns>The remove outcome with the previous value when one was observed.</returns>
        internal Task<CacheRemoveResult<object?>> CommitRemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key),
            static write => ReplicaOperationFingerprints.Remove(write.OperationId, write.CacheName, write.Key),
            static (factory, write, index, token) => new ValueTask<PreparedReplicaMutation>(factory.PrepareRemoveAsync(write.OperationId, write.CacheName, write.Key, index, token)),
            static outcome => new ValueTask<CacheRemoveResult<object?>>(ReplicaOutcomeCodec.DecodeRemoveAsync(outcome)),
            cancellationToken);

        /// <summary>Commits a replicated expiration removal.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns><see langword="true" /> when an expiration was present and cleared.</returns>
        internal Task<bool> CommitRemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key),
            static write => ReplicaOperationFingerprints.RemoveExpiration(write.OperationId, write.CacheName, write.Key),
            static (factory, write, index, token) => new ValueTask<PreparedReplicaMutation>(factory.PrepareRemoveExpirationAsync(write.OperationId, write.CacheName, write.Key, index, token)),
            static outcome => new ValueTask<bool>(ReplicaOutcomeCodec.DecodeApplied(outcome)),
            cancellationToken);

        /// <summary>Commits a replicated unconditional writing.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="entry">Entry to write.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns>A task that completes after the commit.</returns>
        internal Task CommitSetAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key, Entry: entry),
            static write => ReplicaOperationFingerprints.Set(write.OperationId, write.CacheName, write.Key, write.Entry),
            static (factory, write, index, _) => new ValueTask<PreparedReplicaMutation>(factory.PrepareSet(write.OperationId, write.CacheName, write.Key, write.Entry, index)),
            static _ => new ValueTask<bool>(true),
            cancellationToken);

        /// <summary>Commits a replicated conditional expiration refresh.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="expiration">New expiration.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns><see langword="true" /> when the key exists and the expiration was refreshed.</returns>
        internal Task<bool> CommitTouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key, Expiration: expiration),
            static write => ReplicaOperationFingerprints.Touch(write.OperationId, write.CacheName, write.Key, write.Expiration),
            static (factory, write, index, token) => new ValueTask<PreparedReplicaMutation>(
                factory.PrepareTouchAsync(write.OperationId, write.CacheName, write.Key, write.Expiration, index, token)),
            static outcome => new ValueTask<bool>(ReplicaOutcomeCodec.DecodeApplied(outcome)),
            cancellationToken);

        /// <summary>Commits a replicated conditional adding and returns whether the key was absent.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="entry">Entry to add when absent.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns><see langword="true" /> when the key was absent and the entry was added.</returns>
        internal Task<bool> CommitTryAddAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key, Entry: entry),
            static write => ReplicaOperationFingerprints.AddIfAbsent(write.OperationId, write.CacheName, write.Key, write.Entry),
            static (factory, write, index, token) => new ValueTask<PreparedReplicaMutation>(
                factory.PrepareTryAddAsync(write.OperationId, write.CacheName, write.Key, write.Entry, index, token)),
            static outcome => new ValueTask<bool>(ReplicaOutcomeCodec.DecodeApplied(outcome)),
            cancellationToken);

        /// <summary>Commits a replicated value replacement.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="value">Replacement value.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns><see langword="true" /> when the key exists and the value was replaced.</returns>
        internal Task<bool> CommitUpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => committer.CommitAsync(
            (cacheName, operationId),
            (OperationId: operationId, CacheName: cacheName, Key: key, Value: value),
            static write => ReplicaOperationFingerprints.Update(write.OperationId, write.CacheName, write.Key, write.Value),
            static (factory, write, index, token) => new ValueTask<PreparedReplicaMutation>(
                factory.PrepareUpdateAsync(write.OperationId, write.CacheName, write.Key, write.Value, index, token)),
            static outcome => new ValueTask<bool>(ReplicaOutcomeCodec.DecodeApplied(outcome)),
            cancellationToken);
    }
}
