using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Contracts;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>
/// Binds a cache namespace to the routed cache contract.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class RoutedCacheApi<T> : ICacheApi<T>
{
    private readonly string _cacheName;
    private readonly ILogicalNamespacedCache<T> _namespaced;

    public RoutedCacheApi(ILogicalNamespacedCache<T> namespaced, string cacheName)
    {
        _namespaced = namespaced ?? throw new ArgumentNullException(nameof(namespaced));
        _cacheName = cacheName ?? throw new ArgumentNullException(nameof(cacheName));
    }

    public ValueTask<bool> ContainsAsync(string key, CancellationToken cancellationToken) => _namespaced.ContainsAsync(_cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken) => _namespaced.GetEntryAsync(_cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string key, CancellationToken cancellationToken) =>
        _namespaced.TryGetValueAsync(_cacheName, key, cancellationToken);

    public ValueTask InsertAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _namespaced.SetAsync(_cacheName, key, entry, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken) => _namespaced.RemoveExpirationAsync(_cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken) => _namespaced.RemoveAsync(_cacheName, key, cancellationToken);

    public ValueTask<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken) => _namespaced.TouchAsync(_cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryInsertAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _namespaced.TryAddAsync(_cacheName, key, entry, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string key, CancellationToken cancellationToken) => _namespaced.TryRemoveAsync(_cacheName, key, cancellationToken);
}
