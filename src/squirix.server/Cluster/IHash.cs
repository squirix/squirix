namespace Squirix.Server.Cluster;

/// <summary>Provides a 64-bit non-cryptographic hash function, suitable for consistent hashing/partitioning.</summary>
internal interface IHash
{
    /// <summary>Computes a 64-bit hash for the canonical cache route key without materializing the route-key string.</summary>
    /// <param name="cacheName">The canonical cache name.</param>
    /// <param name="key">The user key.</param>
    /// <returns>The 64-bit hash value.</returns>
    ulong HashCacheRouteKey(string cacheName, string key);

    /// <summary>Computes a 64-bit hash for a virtual node key without materializing the vnode string.</summary>
    /// <param name="node">The physical node identifier.</param>
    /// <param name="index">The virtual node index.</param>
    /// <returns>The 64-bit hash value.</returns>
    ulong HashVNode(string node, int index);
}
