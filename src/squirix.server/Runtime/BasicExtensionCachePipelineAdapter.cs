using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class BasicExtensionCachePipelineAdapter<T> : ISquirixServerEntryCachePipeline<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public BasicExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> inner) => _inner = inner;

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _inner.AddAsync(cacheName, key, value, cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _inner.AddAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.ContainsAsync(cacheName, key, cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _inner.SetAsync(cacheName, key, value, cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _inner.SetAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => _inner.RemoveAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => _inner.TouchAsync(cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _inner.TryAddAsync(cacheName, key, value, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => _inner.TryAddAsync(cacheName, key, entry, cancellationToken);
}
