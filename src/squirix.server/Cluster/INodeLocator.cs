namespace Squirix.Server.Cluster;

/// <summary>
/// Selects the original owner for a cache route key on the vnode consistent-hash ring.
/// Replica followers are resolved separately via <c>IReplicaGroupLocator</c>.
/// </summary>
internal interface INodeLocator
{
    /// <summary>Gets the original owner for a cache route key without materializing the canonical route-key string.</summary>
    /// <param name="cacheName">Canonical cache name for the operation.</param>
    /// <param name="key">User key for the operation.</param>
    /// <returns>The original owner node for the composed route key.</returns>
    string GetOwner(string cacheName, string key);
}
