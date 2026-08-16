using System;
using Squirix.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Memory pressure settings loaded from configuration before RAM budget resolution.</summary>
[Immutable]
internal sealed record UnresolvedMemoryPressureOptions
{
    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="PressureLevel.Critical" />.
    /// </summary>
    internal int CriticalPressureThresholdPercent { get; init; } = 95;

    /// <summary>
    /// Gets the usage percentage at or above which state becomes <see cref="PressureLevel.High" />.
    /// </summary>
    internal int HighPressureThresholdPercent { get; init; } = 80;

    /// <summary>
    /// Gets the optional explicit maximum estimated cache size in bytes.
    /// When unset, startup resolves the limit to <see cref="OptionsResolver.RamBudgetPercent" /> of available memory.
    /// </summary>
    internal long? MaxEstimatedCacheBytes { get; init; }

    /// <summary>Validates unresolved scalars before RAM budget resolution.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a scalar is out of range.</exception>
    internal void Validate()
    {
        if (MaxEstimatedCacheBytes is < 0)
            throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes cannot be negative.");

        ValidatePercent(HighPressureThresholdPercent, "MemoryPressure HighPressureThresholdPercent must be in the range (0, 100].");
        ValidatePercent(CriticalPressureThresholdPercent, "MemoryPressure CriticalPressureThresholdPercent must be in the range (0, 100].");
    }

    private static void ValidatePercent(int value, string outOfRangeMessage)
    {
        if (value is <= 0 or > 100)
            throw new InvalidOperationException(outOfRangeMessage);
    }
}
