using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Mutating operations for the process-local physical cache store.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheMutationOperations<T>
{
    ValueTask<CacheRemoveResult<T>> RemoveAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken);

    ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken);
}
