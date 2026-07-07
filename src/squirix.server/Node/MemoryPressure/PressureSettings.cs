using System.Text.Json.Serialization;
using JetBrains.Annotations;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Partial settings shape for <c>Squirix.settings.json</c> <c>MemoryPressure</c> section (nullable fields merge onto defaults).
/// </summary>
[UsedImplicitly]
internal sealed class PressureSettings
{
    [UsedImplicitly]
    [JsonPropertyName("highPressureThresholdPercent")]
    public int? HighPressureThresholdPercent { get; init; }

    [UsedImplicitly]
    [JsonPropertyName("maxEstimatedCacheBytes")]
    public long? MaxEstimatedCacheBytes { get; init; }

    [UsedImplicitly]
    [JsonPropertyName("criticalPressureThresholdPercent")]
    public int? CriticalPressureThresholdPercent { get; init; }

    /// <summary>
    /// Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).
    /// </summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    internal UnresolvedMemoryPressureOptions MergeInto(UnresolvedMemoryPressureOptions baseline) => new()
    {
        MaxEstimatedCacheBytes = MaxEstimatedCacheBytes ?? baseline.MaxEstimatedCacheBytes,
        HighPressureThresholdPercent = HighPressureThresholdPercent ?? baseline.HighPressureThresholdPercent,
        CriticalPressureThresholdPercent = CriticalPressureThresholdPercent ?? baseline.CriticalPressureThresholdPercent,
    };
}
