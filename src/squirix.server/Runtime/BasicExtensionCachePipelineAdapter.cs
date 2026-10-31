using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

[Immutable]
internal sealed class BasicExtensionCachePipelineAdapter<T> : ISquirixServerEntryCachePipeline<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    internal BasicExtensionCachePipelineAdapter(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner;
    }

    public async ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await _inner.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return entry is { Value: JsonUtf8Value }
            ? new NodeCacheEntry<T>(ForExtension(entry.Value), entry.Version, entry.ExpiresUtc, entry.Expiration, entry.Tags)
            : entry;
    }

    public async ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var result = await _inner.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        return result.Value is JsonUtf8Value ? new NodeCacheValueResult<T>(result.Found, ForExtension(result.Value)) : result;
    }

    public async ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        var result = await _inner.RemoveAsync(operationId, cacheName, key, cancellationToken).ConfigureAwait(false);
        return result.Value is JsonUtf8Value ? new CacheRemoveResult<T>(result.Removed, ForExtension(result.Value)) : result;
    }

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

    /// <summary>Shows a structured value to an extension as the <see cref="JsonElement" /> its contract promises, instead of the internal JSON bytes.</summary>
    /// <param name="value">The stored value.</param>
    /// <returns>The element for a structured value when <typeparamref name="T" /> can hold it, otherwise the value unchanged.</returns>
    private static T? ForExtension(T? value)
    {
        if (value is not JsonUtf8Value json)
            return value;

        object element = json.ToJsonElement();
        return element is T converted ? converted : value;
    }
}
