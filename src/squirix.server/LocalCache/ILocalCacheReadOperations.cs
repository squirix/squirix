using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Read-oriented operations for the process-local physical cache store.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheReadOperations<T>
{
    NodeCacheEntry<T>? GetEntry(CacheKey key, CancellationToken cancellationToken);

    NodeCacheValueResult<T> GetValue(CacheKey key, CancellationToken cancellationToken);

    /// <summary>Reads the value of a key with the deadline of its entry, as <see cref="GetValue" /> reads the value, without building the entry.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The value and deadline of the entry, or a miss.</returns>
    NodeCacheValueRead<T> GetValueWithDeadline(CacheKey key, CancellationToken cancellationToken);
}
