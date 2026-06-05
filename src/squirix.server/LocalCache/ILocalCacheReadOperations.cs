using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Read-oriented operations for the process-local physical cache store.
/// </summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheReadOperations<T>
{
    ValueTask<bool> ContainsAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<TimeSpan?> GetExpirationAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<CacheEntry<T>?> GetValueAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> TryGetValueAsync(CacheKey key, CancellationToken cancellationToken);
}
