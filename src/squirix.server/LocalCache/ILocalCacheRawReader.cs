using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Observes physical entries, including expired entries, without mutating local state.</summary>
/// <typeparam name="T">Stored value type.</typeparam>
internal interface ILocalCacheRawReader<T>
{
    /// <summary>Reads an entry without applying lazy expiration.</summary>
    /// <param name="key">Physical cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The raw entry, or <see langword="null" /> when absent.</returns>
    ValueTask<NodeCacheEntry<T>?> GetEntryRawAsync(CacheKey key, CancellationToken cancellationToken);
}
