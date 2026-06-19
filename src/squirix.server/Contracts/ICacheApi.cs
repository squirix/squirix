using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Contracts;

/// <summary>Entry-based cache API for a single namespace; gRPC handlers translate wire requests into these operations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ICacheApi<T>
{
    ValueTask<CacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken);

    ValueTask SetEntryAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> TryAddEntryAsync(string key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<CacheRemoveResult<T>> RemoveAsync(string key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken);
}
