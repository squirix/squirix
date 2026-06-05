using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Entry-aware logical cache pipeline surface available to integrations.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
public interface ISquirixServerEntryCachePipeline<T> : ISquirixServerCachePipeline<T>
{
    /// <summary>Adds an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="entry">Entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the entry is added.</returns>
    ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Gets an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The entry, or null when missing.</returns>
    ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken);

    /// <summary>Inserts an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="entry">Entry to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the entry is inserted.</returns>
    ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    /// <summary>Attempts to add an entry.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="entry">Entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the entry was added.</returns>
    ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);
}
