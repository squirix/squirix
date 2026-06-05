using System;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Configures memory pressure observation and admission protection.
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
                throw new ArgumentOutOfRangeException(nameof(CriticalPressureThresholdPercent), value, "CriticalPressureThresholdPercent must be in the range (0, 100].");

            field = value;
        }
    }

    /// <summary>
    /// Gets a value indicating whether memory pressure evaluation is active.
    /// When <see langword="false" />, state remains <see cref="MemoryPressureState.Normal" /> regardless of usage.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="MemoryPressureState.High" />.
    /// </summary>
    public int HighPressureThresholdPercent
    {
        get;
        init
        {
            if (value is <= 0 or > 100)
                throw new ArgumentOutOfRangeException(nameof(HighPressureThresholdPercent), value, "HighPressureThresholdPercent must be in the range (0, 100].");

            field = value;
        }
    }

    /// <summary>
    /// Gets the optional maximum estimated cache size in bytes used for pressure thresholds.
    /// <see langword="null" /> or non-positive values mean no limit is configured (no pressure classification).
    /// </summary>
    public long? MaxEstimatedCacheBytes
    {
        get;
        init
        {
            if (value is < 0)
                throw new ArgumentOutOfRangeException(nameof(MaxEstimatedCacheBytes), value, "MaxEstimatedCacheBytes cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets a value indicating whether writes should be rejected under critical pressure when admission control is implemented.
    /// </summary>
    public bool RejectWritesOnCriticalPressure { get; init; } = true;

    /// <summary>
    /// Validates configuration; throws <see cref="InvalidOperationException" /> when invalid.
    /// </summary>
    public void Validate()
    {
        if (MaxEstimatedCacheBytes is < 0)
            throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes cannot be negative.");

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
