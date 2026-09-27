using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Per-node memory pressure overrides for in-process test hosts.
/// Unset members keep the server defaults; the server validates the resulting options at startup.
/// </summary>
[Immutable]
public sealed class TestNodeMemoryPressureOptions
{
    /// <summary>Gets the usage percentage that enters critical pressure, or <see langword="null" /> for the server default.</summary>
    public int? CriticalPressureThresholdPercent { get; init; }

    /// <summary>Gets the usage percentage that enters high pressure, or <see langword="null" /> for the server default.</summary>
    public int? HighPressureThresholdPercent { get; init; }

    /// <summary>Gets the maximum estimated cache size in bytes, or <see langword="null" /> for the server default.</summary>
    public long? MaxEstimatedCacheBytes { get; init; }

    internal PressureOptions ToServerOptions()
    {
        var defaults = new PressureOptions();
        return defaults with
        {
            CriticalPressureThresholdPercent = CriticalPressureThresholdPercent ?? defaults.CriticalPressureThresholdPercent,
            HighPressureThresholdPercent = HighPressureThresholdPercent ?? defaults.HighPressureThresholdPercent,
            MaxEstimatedCacheBytes = MaxEstimatedCacheBytes ?? defaults.MaxEstimatedCacheBytes,
        };
    }
}
