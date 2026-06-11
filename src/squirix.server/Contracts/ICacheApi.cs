using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Contracts;

/// <summary>
/// Transport-facing cache API for a single namespace.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ICacheApi<T>
{
    ValueTask<bool> ContainsAsync(string key, CancellationToken cancellationToken);

    ValueTask<CacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> TryGetValueAsync(string key, CancellationToken cancellationToken);

    ValueTask InsertAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> TryInsertAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string key, CancellationToken cancellationToken);
}
