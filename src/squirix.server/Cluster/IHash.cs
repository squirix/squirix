namespace Squirix.Server.Cluster;

/// <summary>
/// Provides a 64-bit non-cryptographic hash function, suitable for consistent hashing/partitioning.
/// </summary>
internal interface IHash
{
    /// <summary>
    /// Computes a 64-bit hash for the canonical cache route key without materializing the route-key string.
    /// </summary>
    /// <param name="cacheName">The canonical cache name.</param>
    /// <param name="key">The user key.</param>
    /// <returns>The 64-bit hash value.</returns>
    ulong HashCacheRouteKey(string cacheName, string key);

    /// <summary>
    /// Computes a 64-bit hash for the specified text encoded as UTF-8.
    /// Hot path: implementations must avoid allocations.
    /// </summary>
    /// <param name="text">The text to hash.</param>
    /// <returns>The 64-bit hash value.</returns>
    ulong HashString(string text);
}
