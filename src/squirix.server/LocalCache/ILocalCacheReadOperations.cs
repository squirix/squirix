using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Read-oriented operations for the process-local physical cache store.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheReadOperations<T>
{
    ValueTask<NodeCacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<NodeCacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken);

    /// <summary>Reads the value of a key with the deadline of its entry, as <see cref="GetValueAsync" /> reads the value, without building the entry.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The value and deadline of the entry, or a miss.</returns>
    ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(CacheKey key, CancellationToken cancellationToken);
}
