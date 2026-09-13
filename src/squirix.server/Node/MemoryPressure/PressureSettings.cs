using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>JSON-serializable memory pressure settings section.</summary>
[Immutable]
internal sealed class PressureSettings
{
    [JsonInclude]
    [JsonPropertyName("criticalPressureThresholdPercent")]
    internal int? CriticalPressureThresholdPercent { get; init; }

    [JsonInclude]
    [JsonPropertyName("highPressureThresholdPercent")]
    internal int? HighPressureThresholdPercent { get; init; }

    [JsonInclude]
    [JsonPropertyName("maxEstimatedCacheBytes")]
    internal long? MaxEstimatedCacheBytes { get; init; }

    /// <summary>Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).</summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    internal UnresolvedMemoryPressureOptions MergeInto(UnresolvedMemoryPressureOptions baseline) => new()
    {
        MaxEstimatedCacheBytes = MaxEstimatedCacheBytes ?? baseline.MaxEstimatedCacheBytes,
        HighPressureThresholdPercent = HighPressureThresholdPercent ?? baseline.HighPressureThresholdPercent,
        CriticalPressureThresholdPercent = CriticalPressureThresholdPercent ?? baseline.CriticalPressureThresholdPercent,
    };
}
