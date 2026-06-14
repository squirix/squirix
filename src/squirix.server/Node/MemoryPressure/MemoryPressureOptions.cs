using System;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Resolved runtime memory pressure configuration.
/// </summary>
internal sealed record MemoryPressureOptions
{
    public MemoryPressureOptions()
    {
        HighPressureThresholdPercent = 80;
        CriticalPressureThresholdPercent = 95;
    }

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="MemoryPressureState.Critical" />.
    /// </summary>
    public int CriticalPressureThresholdPercent
    {
        get;
        init
        {
            if (value is <= 0 or > 100)
                throw new ArgumentOutOfRangeException(nameof(value), value, "CriticalPressureThresholdPercent must be in the range (0, 100].");

            field = value;
        }
    }

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="MemoryPressureState.High" />.
    /// </summary>
    public int HighPressureThresholdPercent
    {
        get;
        init
        {
            if (value is <= 0 or > 100)
                throw new ArgumentOutOfRangeException(nameof(value), value, "HighPressureThresholdPercent must be in the range (0, 100].");

            field = value;
        }
    }

    /// <summary>
    /// Gets the maximum estimated cache size in bytes used for pressure thresholds.
    /// </summary>
    public long MaxEstimatedCacheBytes
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MaxEstimatedCacheBytes must be positive.");

            field = value;
        }
    }

    /// <summary>
    /// Validates configuration; throws <see cref="InvalidOperationException" /> when invalid.
    /// </summary>
    public void Validate()
    {
        if (MaxEstimatedCacheBytes <= 0)
            throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes must be positive.");

        ValidatePercent(nameof(HighPressureThresholdPercent), HighPressureThresholdPercent);
        ValidatePercent(nameof(CriticalPressureThresholdPercent), CriticalPressureThresholdPercent);

        if (HighPressureThresholdPercent >= CriticalPressureThresholdPercent)
        {
            throw new InvalidOperationException("MemoryPressure HighPressureThresholdPercent must be less than CriticalPressureThresholdPercent.");
        }
    }

    private static void ValidatePercent(string name, int value)
    {
        if (value is <= 0 or > 100)
            throw new InvalidOperationException($"MemoryPressure {name} must be in the range (0, 100].");
    }
}
