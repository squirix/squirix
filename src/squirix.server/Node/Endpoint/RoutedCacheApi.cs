using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Binds a cache namespace to the routed cache contract.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class RoutedCacheApi<T> : ICacheApi<T>
{
    private readonly string _cacheName;
    private readonly ILogicalNamespacedCache<T> _namespaced;

    internal RoutedCacheApi(ILogicalNamespacedCache<T> namespaced, string cacheName)
    {
        ArgumentNullException.ThrowIfNull(namespaced);
        ArgumentNullException.ThrowIfNull(cacheName);
        _namespaced = namespaced;
        _cacheName = cacheName;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null) => _namespaced.GetEntryForRequestAsync(_cacheName, key, cancellationToken, context);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.GetValueWithDeadlineForRequestAsync(_cacheName, key, cancellationToken, context);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null) => _namespaced.GetValueForRequestAsync(_cacheName, key, cancellationToken, context);

    public ValueTask<bool> RemoveAsync(string operationId, string key, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.RemoveForRequestAsync(operationId, _cacheName, key, cancellationToken, context);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string key, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.RemoveExpirationForRequestAsync(operationId, _cacheName, key, cancellationToken, context);

    public ValueTask SetEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.SetEntryForRequestAsync(operationId, _cacheName, key, entry, cancellationToken, context);

    public ValueTask<bool> TouchAsync(string operationId, string key, TimeSpan expiration, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.TouchForRequestAsync(operationId, _cacheName, key, expiration, cancellationToken, context);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.TryAddEntryForRequestAsync(operationId, _cacheName, key, entry, cancellationToken, context);

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.TryReplaceEntryForRequestAsync(operationId, _cacheName, key, entry, cancellationToken, context);

    public ValueTask<bool> UpdateAsync(string operationId, string key, T? value, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.UpdateForRequestAsync(operationId, _cacheName, key, value, cancellationToken, context);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        _namespaced.AddOrGetForRequestAsync(operationId, _cacheName, key, entry, cancellationToken, context);
}
