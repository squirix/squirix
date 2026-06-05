using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Bounded deterministic approximate byte estimate for a live cache entry (v0.7.x). Not exact CLR heap usage.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ICacheEntrySizeEstimator<T>
{
    /// <summary>
    /// Estimates stored footprint using namespace/key lengths, tags, version/expiration metadata, and typed or counter payload hints.
    /// </summary>
    /// <param name="key">The cache key.</param>
    /// <param name="entry">The observed cache entry snapshot.</param>
    /// <param name="payloadIsCounter">When <see langword="true" />, the payload uses the dedicated counter representation.</param>
    /// <returns>The approximate byte footprint for the entry.</returns>
    long EstimateBytes(CacheKey key, CacheEntry<T> entry, bool payloadIsCounter);
}
