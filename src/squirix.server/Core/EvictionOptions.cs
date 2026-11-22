using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>Options controlling capacity-based eviction behavior of the in-memory cache.</summary>
[Immutable]
internal sealed class EvictionOptions
{
    /// <summary>
    /// Gets the maximum number of live entries before evictions are triggered.
    /// A value of <see langword="null" /> disables capacity-based eviction.
    /// </summary>
    internal int? Capacity { get; init; }

    /// <summary>Gets or initializes a value indicating whether to enable proactive eviction at the node's high estimated memory threshold.</summary>
    internal bool EvictOnMemoryPressure { get; init; }

    internal bool Enabled => Capacity != null || EvictOnMemoryPressure;

    /// <summary>
    /// Gets the eviction policy to use when capacity is exceeded.
    /// Defaults to <see cref="EvictionPolicyType.Lru" />.
    /// </summary>
    internal EvictionPolicyType Policy { get; init; } = EvictionPolicyType.Lru;
}
