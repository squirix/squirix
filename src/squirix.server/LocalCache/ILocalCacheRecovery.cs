using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Trusted replay entry points used during durable recovery.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheRecovery<T>
{
    /// <summary>Replays an entry.</summary>
    /// <param name="key">The cache key.</param>
    /// <param name="entry">The entry to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="encodedLength">The journal-encoded length of <paramref name="entry" /> when the recovery knows it; <see langword="null" /> to measure.</param>
    /// <returns>A task that completes when the entry is stored.</returns>
    ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null);

    ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken);
}
