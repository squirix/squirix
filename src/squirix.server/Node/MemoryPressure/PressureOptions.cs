using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Resolved runtime memory pressure configuration.</summary>
internal sealed record PressureOptions
{
    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="PressureLevel.High" />.
    /// </summary>
    [JsonInclude]
    internal int HighPressureThresholdPercent { get; init; } = 80;

    /// <summary>Gets the maximum estimated cache size in bytes used for pressure thresholds.</summary>
    [JsonInclude]
    internal long MaxEstimatedCacheBytes { get; init; }

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="PressureLevel.Critical" />.
    /// </summary>
    [JsonInclude]
    internal int CriticalPressureThresholdPercent { get; init; } = 95;

    /// <summary>
    /// Validates configuration; throws <see cref="InvalidOperationException" /> when invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when thresholds or cache byte limits are invalid.</exception>
    internal void Validate()
    {
        if (MaxEstimatedCacheBytes <= 0)
            throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes must be positive.");

        ValidatePercent(nameof(HighPressureThresholdPercent), HighPressureThresholdPercent);
        ValidatePercent(nameof(CriticalPressureThresholdPercent), CriticalPressureThresholdPercent);

        if (HighPressureThresholdPercent >= CriticalPressureThresholdPercent)
            throw new InvalidOperationException("MemoryPressure HighPressureThresholdPercent must be less than CriticalPressureThresholdPercent.");
    }

    private static void ValidatePercent(string name, int value)
    {
        if (value is <= 0 or > 100)
            throw new InvalidOperationException($"MemoryPressure {name} must be in the range (0, 100].");
    }
}
