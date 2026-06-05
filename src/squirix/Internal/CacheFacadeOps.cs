using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal;

internal static class CacheFacadeOps
{
    public static Task<CacheValueResult<T>> GetOrAddAsync<T>(
        ICache<T> cache,
        KeyedSingleFlight flights,
        string key,
        Func<string, CancellationToken, Task<T?>> valueFactory,
        CacheEntryOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(flights);
        ArgumentNullException.ThrowIfNull(valueFactory);

        return flights.RunAsync(
            key,
            async ct =>
            {
                var existing = await cache.GetValueAsync(key, ct).ConfigureAwait(false);
                if (existing.Found)
                    return existing;

                var created = await valueFactory(key, ct).ConfigureAwait(false);

                if (await cache.TryAddAsync(key, created, options, ct).ConfigureAwait(false))
                    return new CacheValueResult<T>(true, created);

                var afterRace = await cache.GetValueAsync(key, ct).ConfigureAwait(false);
                return afterRace.Found ? afterRace : new CacheValueResult<T>(true, created);
            },
            cancellationToken);
    }

    public static async Task<bool> UpdateAsync<T>(ICache<T> cache, string key, T? value, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cache);

        var existing = await cache.GetEntryAsync(key, cancellationToken).ConfigureAwait(false);
        if (!existing.Found)
            return false;

        var entry = existing.Entry!;
        CacheEntryOptions? options = null;
        if (entry.Expiration is not null)
            options = new CacheEntryOptions { Expiration = entry.Expiration };
        else if (entry.ExpiresUtc is not null)
            options = new CacheEntryOptions { ExpiresAt = DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc) };

        await cache.SetAsync(key, value, options, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
