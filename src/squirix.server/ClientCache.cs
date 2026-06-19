using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server;

/// <summary>Adapts the process-local physical cache to the logical namespaced contract.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ClientCache<T> : ILogicalNamespacedCache<T>
{
    private readonly ILocalCacheMutationOperations<T> _mutation;
    private readonly ILocalCacheReadOperations<T> _read;

    public ClientCache(ILocalCacheReadOperations<T> read, ILocalCacheMutationOperations<T> mutation)
    {
        _read = read ?? throw new ArgumentNullException(nameof(read));
        _mutation = mutation ?? throw new ArgumentNullException(nameof(mutation));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetEntryAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetValueAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _mutation.RemoveAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _mutation.RemoveExpirationAsync(Key(cacheName, key), cancellationToken);

    public async ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var cacheKey = Key(cacheName, key);
        if (entry.ExpiresUtc is null && entry.Expiration is null)
        {
            var existing = await _read.GetEntryAsync(cacheKey, cancellationToken).ConfigureAwait(false);
            if (existing is not null)
                entry = CacheEntryUpdatePolicy.PreserveExpirationWhenNotSpecified(entry, existing);
        }

        await _mutation.SetAsync(cacheKey, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _mutation.TouchAsync(Key(cacheName, key), expiration, cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _mutation.TryAddAsync(Key(cacheName, key), entry, cancellationToken);

    public async ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var cacheKey = Key(cacheName, key);
        return await _mutation.UpdateAsync(cacheKey, value, cancellationToken).ConfigureAwait(false);
    }

    private static CacheKey Key(string cacheName, string key) => new(cacheName, key);
}
