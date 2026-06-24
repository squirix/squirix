using System;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Memory pressure settings loaded from configuration before RAM budget resolution.</summary>
internal sealed record UnresolvedMemoryPressureOptions
{
    public UnresolvedMemoryPressureOptions()
    {
        HighPressureThresholdPercent = 80;
        CriticalPressureThresholdPercent = 95;
    }

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="MemoryPressureState.Critical" />.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not in the range (0, 100].</exception>
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
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not in the range (0, 100].</exception>
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
    /// Gets the optional explicit maximum estimated cache size in bytes.
    /// When unset, startup resolves the limit to <see cref="MemoryPressureOptionsResolver.RamBudgetPercent" /> of available memory.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    public long? MaxEstimatedCacheBytes
    {
        get;
        init
        {
            if (value is < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MaxEstimatedCacheBytes cannot be negative.");

            field = value;
        }
    }
}
