using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Basic logical cache pipeline surface available to integrations.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
public interface ISquirixServerCachePipeline<T>
{
    /// <summary>Adds a value.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the value is added.</returns>
    ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);

    /// <summary>Returns whether a key exists.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key exists.</returns>
    ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Gets a remaining time to live.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The remaining expiration, if the key has one.</returns>
    ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Gets a value.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The value, or null when missing.</returns>
    ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Inserts a value.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the value is inserted.</returns>
    ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);

    /// <summary>Removes an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key was removed.</returns>
    ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Updates expiration for an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="expiration">New time to live.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key was found.</returns>
    ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken);

    /// <summary>Attempts to add a value.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the value was added.</returns>
    ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);
}
