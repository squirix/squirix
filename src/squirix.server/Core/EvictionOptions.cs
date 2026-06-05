namespace Squirix.Server.Core;

/// <summary>
/// Options controlling capacity-based eviction behavior of the in-memory cache.
/// </summary>
internal sealed class EvictionOptions
{
    /// <summary>
    /// Gets the maximum number of live entries before evictions are triggered.
    /// A value of <c>null</c> disables capacity-based eviction.
    /// </summary>
    public int? Capacity { get; init; }

    /// <summary>
    /// Gets the eviction policy to use when capacity is exceeded.
    /// Defaults to <see cref="EvictionPolicyType.Lru" />.
    /// </summary>
    public EvictionPolicyType Policy { get; init; } = EvictionPolicyType.Lru;
}
