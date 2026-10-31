using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

[Immutable]
internal sealed class ExtensionCachePipelineAdapter<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _core;
    private readonly ISquirixServerEntryCachePipeline<T>? _pipeline;

    internal ExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> core, ISquirixServerCachePipeline decorated)
    {
        ArgumentNullException.ThrowIfNull(core);
        ArgumentNullException.ThrowIfNull(decorated);
        _core = core;
        _pipeline = decorated as ISquirixServerEntryCachePipeline<T>;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline == null ? _core.GetEntryAsync(cacheName, key, cancellationToken) : GetEntryThroughPipelineAsync(_pipeline, cacheName, key, cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline == null ? _core.GetValueAsync(cacheName, key, cancellationToken) : GetValueThroughPipelineAsync(_pipeline, cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline == null ? _core.RemoveAsync(operationId, cacheName, key, cancellationToken) : RemoveThroughPipelineAsync(_pipeline, operationId, cacheName, key, cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _pipeline?.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken) ?? _core.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.SetEntryAsync(operationId, cacheName, key, ForExtension(entry), cancellationToken) ?? _core.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _pipeline?.TouchAsync(operationId, cacheName, key, expiration, cancellationToken) ?? _core.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _pipeline?.TryAddEntryAsync(operationId, cacheName, key, ForExtension(entry), cancellationToken) ?? _core.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _pipeline?.UpdateAsync(operationId, cacheName, key, ForExtension(value), cancellationToken) ?? _core.UpdateAsync(operationId, cacheName, key, value, cancellationToken);

    private static async ValueTask<NodeCacheEntry<T>?> GetEntryThroughPipelineAsync(ISquirixServerEntryCachePipeline<T> pipeline, string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await pipeline.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return entry is { Value: JsonElement } ? new NodeCacheEntry<T>(FromExtension(entry.Value), entry.Version, entry.ExpiresUtc, entry.Expiration, entry.Tags) : entry;
    }

    private static async ValueTask<NodeCacheValueResult<T>> GetValueThroughPipelineAsync(ISquirixServerEntryCachePipeline<T> pipeline, string cacheName, string key, CancellationToken cancellationToken)
    {
        var result = await pipeline.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return result.Value is JsonElement ? new NodeCacheValueResult<T>(result.Found, FromExtension(result.Value)) : result;
    }

    private static async ValueTask<CacheRemoveResult<T>> RemoveThroughPipelineAsync(ISquirixServerEntryCachePipeline<T> pipeline, string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var result = await pipeline.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        return result.Value is JsonElement ? new CacheRemoveResult<T>(result.Removed, FromExtension(result.Value)) : result;
    }

    private static NodeCacheEntry<T> ForExtension(NodeCacheEntry<T> entry) =>
        entry.Value is JsonUtf8Value ? new NodeCacheEntry<T>(ForExtension(entry.Value), entry.Version, entry.ExpiresUtc, entry.Expiration, entry.Tags) : entry;

    /// <summary>Shows a structured value to an extension as the <see cref="JsonElement" /> its contract promises, instead of the internal JSON bytes.</summary>
    /// <param name="value">The value the node holds.</param>
    /// <returns>The element for a structured value when <typeparamref name="T" /> can hold it, otherwise the value unchanged.</returns>
    private static T? ForExtension(T? value)
    {
        if (value is not JsonUtf8Value json)
            return value;

        object element = json.ToJsonElement();
        return element is T converted ? converted : value;
    }

    /// <summary>Takes a value back from an extension as the internal form, keeping the raw JSON text of a structured element.</summary>
    /// <param name="value">The value the extension returned.</param>
    /// <returns>The normalized value when <typeparamref name="T" /> can hold it, otherwise the value unchanged.</returns>
    private static T? FromExtension(T? value)
    {
        if (value is not JsonElement)
            return value;

        var normalized = NodeCacheEntry<T>.NormalizeValue(value);
        return normalized is T converted ? converted : value;
    }
}
