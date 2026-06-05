using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Read-only enumeration of live local entries for snapshot and similar maintenance paths.
/// </summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheSnapshotReader<T>
{
    /// <summary>
    /// Enumerates all live (non-expired) entries currently held in the cache.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while enumerating.</param>
    /// <returns>An asynchronous sequence of key/entry pairs representing the live contents of the cache.</returns>
    IAsyncEnumerable<(CacheKey Key, CacheEntry<T> Entry)> EnumerateLiveAsync(CancellationToken cancellationToken);
}
