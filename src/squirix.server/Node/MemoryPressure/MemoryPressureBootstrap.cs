using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Loads <see cref="MemoryPressureOptions" /> from <c>Squirix.settings.json</c> and environment variables.
/// </summary>
internal static class MemoryPressureBootstrap
{
    /// <summary>
    /// Loads memory pressure options using the same settings file discovery as cluster bootstrap, then applies environment overrides.
    /// </summary>
    /// <returns>Resolved options (not yet validated).</returns>
    public static MemoryPressureOptions Load()
    {
        var baseline = new MemoryPressureOptions();
        _ = UnifiedSettings.TryMergeMemoryPressureFromFile(baseline, out var fileMerged);
        return ApplyEnvironment(fileMerged);
    }

    private static MemoryPressureOptions ApplyEnvironment(MemoryPressureOptions options)
    {
        var result = options;

        var enabled = EnvVariables.ReadExplicitBool("SQUIRIX_MEMORY_PRESSURE_ENABLED");
        if (enabled.HasValue)
            result = result with { Enabled = enabled.Value };

        var maxBytes = EnvVariables.ReadInt64("SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES");
        if (maxBytes.HasValue)
            result = result with { MaxEstimatedCacheBytes = maxBytes.Value <= 0 ? null : maxBytes };

        var high = EnvVariables.ReadInt("SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT");
        if (high.HasValue)
            result = result with { HighPressureThresholdPercent = high.Value };

        var critical = EnvVariables.ReadInt("SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT");
        if (critical.HasValue)
            result = result with { CriticalPressureThresholdPercent = critical.Value };

        var rejectWrites = EnvVariables.ReadExplicitBool("SQUIRIX_MEMORY_PRESSURE_REJECT_WRITES_ON_CRITICAL");
        if (rejectWrites.HasValue)
            result = result with { RejectWritesOnCriticalPressure = rejectWrites.Value };

        return result;
    }
}
