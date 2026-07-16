using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class BasicExtensionCachePipelineAdapter<T> : ISquirixServerEntryCachePipeline<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    internal BasicExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
}
