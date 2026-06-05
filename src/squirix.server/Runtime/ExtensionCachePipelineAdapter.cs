using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class ExtensionCachePipelineAdapter<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _core;
    private readonly ISquirixServerCachePipeline<T> _decorated;
    private readonly ISquirixServerEntryCachePipeline<T>? _entryDecorated;

    public ExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> core, ISquirixServerCachePipeline<T> decorated)
    {
        _core = core ?? throw new ArgumentNullException(nameof(core));
        _decorated = decorated ?? throw new ArgumentNullException(nameof(decorated));
        _entryDecorated = decorated as ISquirixServerEntryCachePipeline<T>;
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _decorated.AddAsync(cacheName, key, value, cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _entryDecorated?.AddAsync(cacheName, key, entry, cancellationToken) ?? _core.AddAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _decorated.ContainsAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _entryDecorated?.GetEntryAsync(cacheName, key, cancellationToken) ?? _core.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _decorated.GetExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _decorated.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _decorated.InsertAsync(cacheName, key, value, cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _entryDecorated?.InsertAsync(cacheName, key, entry, cancellationToken) ?? _core.InsertAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _core.RemoveExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => _decorated.RemoveAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => _decorated.TouchAsync(cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _decorated.TryAddAsync(cacheName, key, value, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _entryDecorated?.TryAddAsync(cacheName, key, entry, cancellationToken) ?? _core.TryAddAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => _core.TryGetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => _core.TryRemoveAsync(cacheName, key, cancellationToken);
}
