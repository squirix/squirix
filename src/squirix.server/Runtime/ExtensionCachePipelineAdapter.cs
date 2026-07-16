using System;
using System.Threading;
using System.Threading.Tasks;
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

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.GetEntryAsync(cacheName, key, cancellationToken) ?? _core.GetEntryAsync(cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.GetValueAsync(cacheName, key, cancellationToken) ?? _core.GetValueAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.RemoveAsync(operationId, cacheName, key, cancellationToken) ?? _core.RemoveAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken) ?? _core.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken) ?? _core.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _pipeline?.TouchAsync(operationId, cacheName, key, expiration, cancellationToken) ?? _core.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken) ?? _core.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _pipeline?.UpdateAsync(operationId, cacheName, key, value, cancellationToken) ?? _core.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
}
