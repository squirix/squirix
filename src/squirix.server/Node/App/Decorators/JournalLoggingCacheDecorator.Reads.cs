using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

internal sealed partial class JournalLoggingCacheDecorator<T>
{
    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        string.Equals(cacheName, _orderedReadCacheName, StringComparison.Ordinal)
            ? _executor.ReadUnderMutationLockAsync(
                new CacheKey(cacheName, key),
                _inner,
                static (inner, address, token) => inner.GetEntryAsync(address.Namespace, address.Key, token),
                cancellationToken)
            : _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        string.Equals(cacheName, _orderedReadCacheName, StringComparison.Ordinal)
            ? _executor.ReadUnderMutationLockAsync(
                new CacheKey(cacheName, key),
                _inner,
                static (inner, address, token) => inner.GetValueAsync(address.Namespace, address.Key, token),
                cancellationToken)
            : _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        string.Equals(cacheName, _orderedReadCacheName, StringComparison.Ordinal)
            ? _executor.ReadUnderMutationLockAsync(
                new CacheKey(cacheName, key),
                _inner,
                static (inner, address, token) => inner.GetValueWithDeadlineAsync(address.Namespace, address.Key, token),
                cancellationToken)
            : _inner.GetValueWithDeadlineAsync(cacheName, key, cancellationToken);

    public async ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        await TryAddEntryAsync(operationId, cacheName, key, entry, request, cancellationToken).ConfigureAwait(false)
            ? new NodeCacheAddOrGetResult<T>(true, default)
            : new NodeCacheAddOrGetResult<T>(false, await GetValueAsync(cacheName, key, request, cancellationToken).ConfigureAwait(false));
}
