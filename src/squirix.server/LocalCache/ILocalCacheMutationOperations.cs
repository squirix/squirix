using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Mutating operations for the process-local physical cache store.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheMutationOperations<T>
{
    ValueTask<CacheRemoveResult<T>> RemoveAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken);

    /// <summary>Stores an entry.</summary>
    /// <param name="key">The cache key.</param>
    /// <param name="entry">The entry to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="encodedLength">
    /// The journal-encoded length of <paramref name="entry" /> as measuring it would return it, when the caller already knows it, so the footprint estimate
    /// does not measure the value again; <see langword="null" /> to measure.
    /// </param>
    /// <returns>A task that completes when the entry is stored.</returns>
    ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null);

    ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken);

    /// <summary>Stores an entry only when the key is absent.</summary>
    /// <param name="key">The cache key.</param>
    /// <param name="entry">The entry to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="encodedLength">The journal-encoded length of <paramref name="entry" /> when the caller already knows it; <see langword="null" /> to measure.</param>
    /// <returns><see langword="true" /> when the entry was stored.</returns>
    ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null);

    ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken);
}
