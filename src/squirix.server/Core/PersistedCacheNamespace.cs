namespace Squirix.Server.Core;

/// <summary>
/// Trusted normalization for cache namespaces read from journal, snapshots, and other persisted payloads.
/// </summary>
internal static class PersistedCacheNamespace
{
    /// <summary>
    /// Returns <see cref="CacheNames.DefaultNamespace" /> when the persisted value is null or empty; otherwise returns the persisted string unchanged.
    /// </summary>
    /// <param name="cacheNamespace">Namespace from persisted storage.</param>
    /// <returns>Canonical namespace string for cache keys.</returns>
    public static string Normalize(string? cacheNamespace) => string.IsNullOrEmpty(cacheNamespace) ? CacheNames.DefaultNamespace : cacheNamespace;
}
