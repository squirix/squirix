using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>
/// Adapts the process-local physical cache to the logical namespaced contract.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class ClientCache<T> : ILogicalNamespacedCache<T>
{
    private readonly KeyedSingleFlight _getOrAddFlights = new();
    private readonly ILocalCacheMutationOperations<T> _mutation;
    private readonly ILocalCacheReadOperations<T> _read;

    public ClientCache(ILocalCacheReadOperations<T> read, ILocalCacheMutationOperations<T> mutation)
    {
        _read = read ?? throw new ArgumentNullException(nameof(read));
        _mutation = mutation ?? throw new ArgumentNullException(nameof(mutation));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => _mutation.AddAsync(Key(cacheName, key), value, cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _mutation.AddAsync(Key(cacheName, key), entry, cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => _read.ContainsAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.GetValueAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _read.GetExpirationAsync(Key(cacheName, key), cancellationToken);

    public async ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var entry = await _read.GetValueAsync(Key(cacheName, key), cancellationToken).ConfigureAwait(false);
        return entry is null ? default : entry.Value;
    }

    public async ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var existing = await TryGetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (existing.Found)
            return existing;

        if (await TryAddAsync(cacheName, key, entry, cancellationToken).ConfigureAwait(false))
            return new CacheValueResult<T>(true, entry.Value);

        return await TryGetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<CacheValueResult<T>> GetOrAddWithFactoryAsync(
        string cacheName,
        string key,
        Func<string, CancellationToken, ValueTask<T?>> valueFactory,
        CacheEntry<T>? entryTemplate,
        CancellationToken cancellationToken) =>
        _getOrAddFlights.RunAsync(
            $"{cacheName}\0{key}",
            async ct =>
            {
                var existing = await TryGetValueAsync(cacheName, key, ct).ConfigureAwait(false);
                if (existing.Found)
                    return existing;

                var created = await valueFactory(key, ct).ConfigureAwait(false);
                var entry = entryTemplate is null
                    ? new CacheEntry<T> { Value = created }
                    : new CacheEntry<T>
                    {
                        Value = created,
                        Expiration = entryTemplate.Expiration,
                        ExpiresUtc = entryTemplate.ExpiresUtc,
                    };

                if (await TryAddAsync(cacheName, key, entry, ct).ConfigureAwait(false))
                    return new CacheValueResult<T>(true, created);

                var afterRace = await TryGetValueAsync(cacheName, key, ct).ConfigureAwait(false);
                return afterRace.Found ? afterRace : new CacheValueResult<T>(true, created);
            },
            cancellationToken);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        SetAsync(cacheName, key, new CacheEntry<T> { Value = value }, cancellationToken);

    public async ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        var cacheKey = Key(cacheName, key);
        if (entry.ExpiresUtc is null && entry.Expiration is null)
        {
            var existing = await _read.GetValueAsync(cacheKey, cancellationToken).ConfigureAwait(false);
            if (existing is not null)
                entry = CacheEntryUpdatePolicy.PreserveExpirationWhenNotSpecified(entry, existing);
        }

        await _mutation.InsertAsync(cacheKey, entry, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => _mutation.RemoveExpirationAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => _mutation.RemoveAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        _mutation.TouchAsync(Key(cacheName, key), expiration, cancellationToken);

    public async ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken)
    {
        var cacheKey = Key(cacheName, key);
        return await _mutation.UpdateAsync(cacheKey, value, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        _mutation.TryAddAsync(Key(cacheName, key), value, cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        _mutation.TryAddAsync(Key(cacheName, key), entry, cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _read.TryGetValueAsync(Key(cacheName, key), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        _mutation.TryRemoveAsync(Key(cacheName, key), cancellationToken);

    private static CacheKey Key(string cacheName, string key) => new(cacheName, key);
}
