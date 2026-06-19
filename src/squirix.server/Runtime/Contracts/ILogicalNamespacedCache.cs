using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Logical namespaced cache surface for the node pipeline.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ILogicalNamespacedCache<T>
{
    ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken);

    ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken);
}
