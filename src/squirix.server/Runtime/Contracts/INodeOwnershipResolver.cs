namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Resolves cache-key ownership for inbound adapter requests.
/// </summary>
internal interface INodeOwnershipResolver
{
    /// <summary>
    /// Gets the current node id.
    /// </summary>
    string SelfNodeId { get; }

    /// <summary>
    /// Resolves the owner node id for the given cache key.
    /// </summary>
    /// <param name="cacheName">Canonical cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>Owner node id.</returns>
    string GetOwner(string cacheName, string key);
}
