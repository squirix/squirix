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
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(mutation);
        _read = read;
        _mutation = mutation;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetEntryAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetValueAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _mutation.RemoveAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        _mutation.RemoveExpirationAsync(Key(cacheName, key), cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _mutation.SetAsync(Key(cacheName, key), entry, cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _mutation.TouchAsync(Key(cacheName, key), expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        _mutation.TryAddAsync(Key(cacheName, key), entry, cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _mutation.UpdateAsync(Key(cacheName, key), value, cancellationToken);

    private static CacheKey Key(string cacheName, string key) => new(cacheName, key);
}
