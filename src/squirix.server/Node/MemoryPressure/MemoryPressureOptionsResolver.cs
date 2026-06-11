using System;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Resolves loaded memory pressure settings against the process RAM budget.
/// </summary>
internal static class MemoryPressureOptionsResolver
{
    /// <summary>
    /// Hard-coded fraction of available process memory used as the default cache limit and maximum configurable limit.
    /// </summary>
    internal const int RamBudgetPercent = 80;

    /// <summary>
    /// Resolves <paramref name="raw" /> into runtime <see cref="MemoryPressureOptions" />.
    /// </summary>
    /// <param name="raw">Loaded settings before RAM resolution.</param>
    /// <param name="budgetProvider">Process memory budget source.</param>
    /// <returns>Validated runtime options with a positive byte limit.</returns>
    /// <exception cref="InvalidOperationException">Resolution or validation failed.</exception>
    public static MemoryPressureOptions Resolve(UnresolvedMemoryPressureOptions raw, IMemoryBudgetProvider budgetProvider)
    {
        ArgumentNullException.ThrowIfNull(raw);
        ArgumentNullException.ThrowIfNull(budgetProvider);

        var availableBytes = budgetProvider.GetTotalAvailableBytes();
        if (availableBytes <= 0)
        {
            throw new InvalidOperationException("MemoryPressure cannot resolve RAM budget: available process memory is zero.");
        }

        var capBytes = ComputeRamCapBytes(availableBytes);
        var maxBytes = raw.MaxEstimatedCacheBytes switch
        {
            null => capBytes,
            <= 0 => throw new InvalidOperationException("MemoryPressure MaxEstimatedCacheBytes must be positive when set."),
            var configured when configured > capBytes => throw new InvalidOperationException(
                $"MemoryPressure MaxEstimatedCacheBytes ({configured}) exceeds the {RamBudgetPercent}% RAM cap ({capBytes})."),
            var configured => (long)configured,
        };

        var options = new MemoryPressureOptions
        {
            MaxEstimatedCacheBytes = maxBytes,
            HighPressureThresholdPercent = raw.HighPressureThresholdPercent,
            CriticalPressureThresholdPercent = raw.CriticalPressureThresholdPercent,
        };
        options.Validate();
        return options;
    }

    private static long ComputeRamCapBytes(long availableBytes)
    {
        if (availableBytes <= 0)
            return 0;

        return availableBytes / 100 * RamBudgetPercent;
    }
}
