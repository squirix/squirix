using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Binds a cache namespace to the routed cache contract.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class RoutedCacheApi<T> : ICacheApi<T>
{
    private readonly string _cacheName;
    private readonly ILogicalNamespacedCache<T> _namespaced;

    internal RoutedCacheApi(ILogicalNamespacedCache<T> namespaced, string cacheName)
    {
        _namespaced = namespaced ?? throw new ArgumentNullException(nameof(namespaced));
        _cacheName = cacheName ?? throw new ArgumentNullException(nameof(cacheName));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken) => _namespaced.GetEntryAsync(_cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken) => _namespaced.GetValueAsync(_cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string key, CancellationToken cancellationToken) => _namespaced.RemoveAsync(operationId, _cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string key, CancellationToken cancellationToken) => _namespaced.RemoveExpirationAsync(operationId, _cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _namespaced.SetEntryAsync(operationId, _cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _namespaced.TouchAsync(operationId, _cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _namespaced.TryAddEntryAsync(operationId, _cacheName, key, entry, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string key, T? value, CancellationToken cancellationToken) => _namespaced.UpdateAsync(operationId, _cacheName, key, value, cancellationToken);
}
