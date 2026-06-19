using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal sealed class ExtensionCachePipelineAdapter<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _core;
    private readonly ISquirixServerEntryCachePipeline<T>? _pipeline;

    public ExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> core, ISquirixServerCachePipeline decorated)
    {
        _core = core ?? throw new ArgumentNullException(nameof(core));
        _ = decorated ?? throw new ArgumentNullException(nameof(decorated));
        _pipeline = decorated as ISquirixServerEntryCachePipeline<T>;
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.GetEntryAsync(cacheName, key, cancellationToken) ?? _core.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.GetValueAsync(cacheName, key, cancellationToken) ?? _core.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.RemoveExpirationAsync(cacheName, key, cancellationToken) ?? _core.RemoveExpirationAsync(cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.SetEntryAsync(cacheName, key, entry, cancellationToken) ?? _core.SetEntryAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _pipeline?.TouchAsync(cacheName, key, expiration, cancellationToken) ?? _core.TouchAsync(cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.TryAddEntryAsync(cacheName, key, entry, cancellationToken) ?? _core.TryAddEntryAsync(cacheName, key, entry, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.RemoveAsync(cacheName, key, cancellationToken) ?? _core.RemoveAsync(cacheName, key, cancellationToken);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _pipeline?.UpdateAsync(cacheName, key, value, cancellationToken) ?? _core.UpdateAsync(cacheName, key, value, cancellationToken);
}
