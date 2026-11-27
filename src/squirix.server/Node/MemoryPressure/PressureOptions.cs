using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Resolved runtime memory pressure configuration.</summary>
[Immutable]
internal sealed record PressureOptions
{
    /// <summary>Gets the usage percentage at or above which state becomes <see cref="PressureLevel.Critical" />.</summary>
    internal int CriticalPressureThresholdPercent { get; init; } = 95;

    /// <summary>Gets the usage percentage at or above which state becomes <see cref="PressureLevel.High" />.</summary>
    internal int HighPressureThresholdPercent { get; init; } = 80;

    /// <summary>Gets the maximum estimated cache size in bytes used for pressure thresholds.</summary>
    internal long MaxEstimatedCacheBytes { get; init; }

    /// <summary>Gets a value indicating whether cache memory accounting is enabled.</summary>
    internal bool MemoryAccountingEnabled { get; init; }

    /// <summary>Gets the first estimated byte count classified as critical.</summary>
    internal long CriticalBytes => ThresholdBytes(CriticalPressureThresholdPercent);

    /// <summary>Gets the first estimated byte count classified as high.</summary>
    internal long HighBytes => ThresholdBytes(HighPressureThresholdPercent);

    /// <summary>Validates configuration; throws <see cref="InvalidOperationException" /> when invalid.</summary>
    /// <exception cref="InvalidOperationException">Thrown when thresholds or cache byte limits are invalid.</exception>
    internal void Validate()
    {
        if (MaxEstimatedCacheBytes <= 0)
            throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes must be positive.");

        ValidatePercent(HighPressureThresholdPercent, "MemoryPressure HighPressureThresholdPercent must be in the range (0, 100].");
        ValidatePercent(CriticalPressureThresholdPercent, "MemoryPressure CriticalPressureThresholdPercent must be in the range (0, 100].");

        if (HighPressureThresholdPercent >= CriticalPressureThresholdPercent)
            throw new InvalidOperationException("MemoryPressure HighPressureThresholdPercent must be less than CriticalPressureThresholdPercent.");
    }

    private static void ValidatePercent(int value, string message)
    {
        if (value is <= 0 or > 100)
            throw new InvalidOperationException(message);
    }

    private long ThresholdBytes(int percent) =>
        ((MaxEstimatedCacheBytes / 100) * percent) + ((((MaxEstimatedCacheBytes % 100) * percent) + 99) / 100);
}
