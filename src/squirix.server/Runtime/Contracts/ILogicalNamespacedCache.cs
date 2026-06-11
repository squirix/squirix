using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Logical namespaced cache surface for the node pipeline.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ILogicalNamespacedCache<T>
{
    ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);

    ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);

    ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);

    ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);
}
