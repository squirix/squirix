using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Logical cache pipeline surface available to integrations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
public interface ISquirixServerEntryCachePipeline<T> : ISquirixServerCachePipeline
{
    /// <summary>Gets an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The entry, or null when missing.</returns>
    ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Gets a value with explicit presence.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The value lookup result.</returns>
    ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Creates or overwrites an entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="entry">Entry to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the entry is stored.</returns>
    ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Attempts to add an entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="entry">Entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the entry was added.</returns>
    ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Removes an entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The remove result.</returns>
    ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Removes expiration from an entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key was found.</returns>
    ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Updates the value of an existing entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="value">New value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key was found and updated.</returns>
    ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken);
}
