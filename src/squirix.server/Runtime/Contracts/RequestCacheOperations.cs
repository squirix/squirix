using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

internal static class RequestCacheOperations
{
    internal static ValueTask<NodeCacheEntry<T>?> GetEntryForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string cacheName, string key, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.GetEntryAsync(cacheName, key, request, cancellationToken)
            : cache.GetEntryAsync(cacheName, key, cancellationToken);

    internal static ValueTask<NodeCacheValueResult<T>> GetValueForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string cacheName, string key, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.GetValueAsync(cacheName, key, request, cancellationToken)
            : cache.GetValueAsync(cacheName, key, cancellationToken);

    internal static ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string cacheName, string key, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.GetValueWithDeadlineAsync(cacheName, key, request, cancellationToken)
            : cache.GetValueWithDeadlineAsync(cacheName, key, cancellationToken);

    internal static ValueTask<bool> RemoveForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.RemoveAsync(operationId, cacheName, key, request, cancellationToken)
            : cache.RemoveAsync(operationId, cacheName, key, cancellationToken);

    internal static ValueTask<bool> RemoveExpirationForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.RemoveExpirationAsync(operationId, cacheName, key, request, cancellationToken)
            : cache.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    internal static ValueTask SetEntryForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.SetEntryAsync(operationId, cacheName, key, entry, request, cancellationToken)
            : cache.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    internal static ValueTask<bool> TouchForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.TouchAsync(operationId, cacheName, key, expiration, request, cancellationToken)
            : cache.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    internal static ValueTask<bool> TryAddEntryForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.TryAddEntryAsync(operationId, cacheName, key, entry, request, cancellationToken)
            : cache.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    internal static ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.AddOrGetAsync(operationId, cacheName, key, entry, request, cancellationToken)
            : cache.AddOrGetAsync(operationId, cacheName, key, entry, cancellationToken);

    internal static ValueTask<bool> TryReplaceEntryForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.TryReplaceEntryAsync(operationId, cacheName, key, entry, request, cancellationToken)
            : cache.TryReplaceEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    internal static ValueTask<bool> UpdateForRequestAsync<T>(this ILogicalNamespacedCache<T> cache, string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken, ICacheRequestContext? request = null) =>
        request != null && cache is IRequestCache<T> contextual
            ? contextual.UpdateAsync(operationId, cacheName, key, value, request, cancellationToken)
            : cache.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
}
