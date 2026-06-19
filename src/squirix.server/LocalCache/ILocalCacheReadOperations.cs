using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Read-oriented operations for the process-local physical cache store.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheReadOperations<T>
{
    ValueTask<CacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<CacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken);
}
