using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>The cache pipeline layers that use inbound request information.</summary>
/// <typeparam name="T">The cached value type.</typeparam>
internal interface IRequestCache<T> : ILogicalNamespacedCache<T>
{
    ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken);

    ValueTask<NodeCacheEntry<T>?> ILogicalNamespacedCache<T>.GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        GetEntryAsync(cacheName, key, null, cancellationToken);

    ValueTask<NodeCacheValueResult<T>> ILogicalNamespacedCache<T>.GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        GetValueAsync(cacheName, key, null, cancellationToken);

    ValueTask<NodeCacheValueRead<T>> ILogicalNamespacedCache<T>.GetValueWithDeadlineAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        GetValueWithDeadlineAsync(cacheName, key, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        RemoveAsync(operationId, cacheName, key, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        RemoveExpirationAsync(operationId, cacheName, key, null, cancellationToken);

    ValueTask ILogicalNamespacedCache<T>.SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        SetEntryAsync(operationId, cacheName, key, entry, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        TouchAsync(operationId, cacheName, key, expiration, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        TryAddEntryAsync(operationId, cacheName, key, entry, null, cancellationToken);

    ValueTask<NodeCacheAddOrGetResult<T>> ILogicalNamespacedCache<T>.AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        AddOrGetAsync(operationId, cacheName, key, entry, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        TryReplaceEntryAsync(operationId, cacheName, key, entry, null, cancellationToken);

    ValueTask<bool> ILogicalNamespacedCache<T>.UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        UpdateAsync(operationId, cacheName, key, value, null, cancellationToken);
}
