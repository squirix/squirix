using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class BasicExtensionCachePipelineAdapter<T> : ISquirixServerEntryCachePipeline<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public BasicExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner;
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _inner.SetEntryAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _inner.TouchAsync(cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _inner.TryAddEntryAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _inner.RemoveExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _inner.UpdateAsync(cacheName, key, value, cancellationToken);
}
