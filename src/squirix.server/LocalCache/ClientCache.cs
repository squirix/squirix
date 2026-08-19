using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.LocalCache;

/// <summary>Adapts the process-local physical cache to the logical namespaced contract.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class ClientCache<T> : ILogicalNamespacedCache<T>
{
    private readonly ILocalCacheMutationOperations<T> _mutation;
    private readonly ILocalCacheReadOperations<T> _read;

    internal ClientCache(ILocalCacheReadOperations<T> read, ILocalCacheMutationOperations<T> mutation)
    {
        _read = read ?? throw new ArgumentNullException(nameof(read));
        _mutation = mutation ?? throw new ArgumentNullException(nameof(mutation));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetEntryAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetValueAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = operationId;
        return _mutation.RemoveAsync(Key(cacheName, key), cancellationToken);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
    {
        _ = operationId;
        return _mutation.RemoveExpirationAsync(Key(cacheName, key), cancellationToken);
    }

    public async ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = operationId;
        var cacheKey = Key(cacheName, key);
        if (entry.ExpiresUtc == null && entry.Expiration == null)
        {
            var existing = await _read.GetEntryAsync(cacheKey, cancellationToken).ConfigureAwait(false);
            if (existing != null)
                entry = PreserveExpirationWhenNotSpecified(entry, existing);
        }

        await _mutation.SetAsync(cacheKey, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        _ = operationId;
        return _mutation.TouchAsync(Key(cacheName, key), expiration, cancellationToken);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        _ = operationId;
        return _mutation.TryAddAsync(Key(cacheName, key), entry, cancellationToken);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        _ = operationId;
        return _mutation.UpdateAsync(Key(cacheName, key), value, cancellationToken);
    }

    private static CacheKey Key(string cacheName, string key) => new(cacheName, key);

    private static NodeCacheEntry<T> PreserveExpirationWhenNotSpecified(NodeCacheEntry<T> replacement, NodeCacheEntry<T> existing)
    {
        if (replacement.ExpiresUtc != null || replacement.Expiration != null)
            return replacement;
        return new NodeCacheEntry<T>(replacement.Value, replacement.Version, existing.ExpiresUtc, null, replacement.Tags ?? existing.Tags);
    }
}
