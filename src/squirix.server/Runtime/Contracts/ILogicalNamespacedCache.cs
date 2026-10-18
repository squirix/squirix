using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Logical namespaced cache surface for the node pipeline.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ILogicalNamespacedCache<T>
{
    ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken);

    ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Stores an entry whose journal-encoded length the caller already knows.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    /// <param name="entry">The entry to store; it carries an absolute deadline or none.</param>
    /// <param name="encodedLength">The journal-encoded length of <paramref name="entry" /> as measuring it would return it; <see langword="null" /> to measure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the entry is stored.</returns>
    /// <remarks>Layers that cannot use the length ignore it and measure the entry.</remarks>
    ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, int? encodedLength, CancellationToken cancellationToken) =>
        SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Stores an entry only when the key is absent, with a journal-encoded length the caller already knows.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    /// <param name="entry">The entry to store; it carries an absolute deadline or none.</param>
    /// <param name="encodedLength">The journal-encoded length of <paramref name="entry" /> as measuring it would return it; <see langword="null" /> to measure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when the entry was stored.</returns>
    /// <remarks>Layers that cannot use the length ignore it and measure the entry.</remarks>
    ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, int? encodedLength, CancellationToken cancellationToken) =>
        TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken);
}
