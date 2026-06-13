using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix;

/// <summary>
/// Consumer-facing API for a strongly typed string-keyed cache implemented by Squirix.
/// </summary>
/// <typeparam name="T">The value type stored in the cache.</typeparam>
/// <remarks>
/// Application code should consume <see cref="ICache{T}" /> rather than implement it directly.
/// Expired entries are treated as absent by all read and mutation methods.
/// When <c>options</c> is <c>null</c>, the implementation applies the cache-configured default expiration.
/// </remarks>
public interface ICache<T>
{
    /// <summary>Adds a new value for the key and throws if a live entry already exists.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to store.</param>
    /// <param name="options">Entry options. When <c>null</c>, the cache-configured default expiration is used.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the value has been added.</returns>
    /// <exception cref="CacheConflictException">Thrown when a live entry already exists for <paramref name="key" />.</exception>
    Task AddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Gets a full entry with presence information. Expired entries are returned as not found.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An entry lookup result.</returns>
    Task<CacheEntryResult<T>> GetEntryAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Gets expiration information for the key. Expired entries are returned as not found.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An expiration lookup result.</returns>
    Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Gets the existing value or adds a value produced by the factory when the key is missing or expired.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="valueFactory">Factory used to produce the value when the key is absent.</param>
    /// <param name="options">Entry options for a produced value. When <c>null</c>, the cache-configured default expiration is used.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The existing or added value result.</returns>
    /// <remarks>
    /// Implementations guarantee that the factory is invoked at most once per key on the local node under concurrent requests.
    /// Distributed single-flight is not guaranteed; the factory may be invoked simultaneously on multiple nodes.
    /// If the factory throws, the exception is propagated to all callers waiting on the same key.
    /// </remarks>
    Task<CacheValueResult<T>> GetOrAddAsync(
        string key,
        Func<string, CancellationToken, Task<T?>> valueFactory,
        CacheEntryOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Gets a value with presence information. Expired entries are returned as not found.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value lookup result.</returns>
    Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Removes a live entry.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a live entry was removed; otherwise <c>false</c>.</returns>
    Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Removes expiration from a live entry.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a live entry expiration was removed; otherwise <c>false</c>.</returns>
    Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Creates or overwrites the value for the key.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to store.</param>
    /// <param name="options">Entry options. When <c>null</c>, the cache-configured default expiration is used.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the value has been stored.</returns>
    Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Updates the expiration of a live entry using the provided relative expiration.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="expiration">Relative expiration to apply.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a live entry was updated; otherwise <c>false</c>.</returns>
    Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default);

    /// <summary>Updates the expiration of a live entry using an absolute expiration time.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="absoluteExpiration">Absolute point in time at which the entry expires.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a live entry was updated; otherwise <c>false</c>.</returns>
    Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default);

    /// <summary>Attempts to add a new value for the key without throwing for an existing live entry.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">Value to store.</param>
    /// <param name="options">Entry options. When <c>null</c>, the cache-configured default expiration is used.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when the value was added; otherwise <c>false</c>.</returns>
    Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Updates the value of a live entry without affecting its expiration.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="value">New value to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a live entry was updated; otherwise <c>false</c>.</returns>
    /// <remarks>
    /// The value update and expiration preservation are performed as a single cache operation.
    /// If the key is missing or expired, no value is written.
    /// </remarks>
    Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default);
}
