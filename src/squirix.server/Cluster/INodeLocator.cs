namespace Squirix.Server.Cluster;

/// <summary>
/// Node locator contract for routing keys to owners (and replicas).
/// </summary>
internal interface INodeLocator
{
    /// <summary>
    /// Gets the owner for a cache route key without materializing the canonical route-key string.
    /// </summary>
    /// <param name="cacheName">Canonical cache name for the operation.</param>
    /// <param name="key">User key for the operation.</param>
    /// <returns>The node that owns the composed route key.</returns>
    string GetOwner(string cacheName, string key);
}
