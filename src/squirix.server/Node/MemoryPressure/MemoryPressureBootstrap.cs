using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Loads unresolved memory pressure settings from <c>Squirix.settings.json</c> and environment variables.
/// </summary>
internal static class MemoryPressureBootstrap
{
    /// <summary>Loads memory pressure settings using the same settings file discovery as cluster bootstrap, then applies environment overrides.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Loaded settings before RAM budget resolution.</returns>
    public static async Task<UnresolvedMemoryPressureOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = new UnresolvedMemoryPressureOptions();
        var (_, fileMerged) = await UnifiedSettings.TryMergeMemoryPressureFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return ApplyEnvironment(fileMerged);
    }

    private static UnresolvedMemoryPressureOptions ApplyEnvironment(UnresolvedMemoryPressureOptions options)
    {
        var result = options;

        var maxBytes = EnvVariables.ReadInt64("SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES");
        if (maxBytes is not null)
            result = result with { MaxEstimatedCacheBytes = maxBytes.Value };

        var high = EnvVariables.ReadInt("SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT");
        if (high is not null)
            result = result with { HighPressureThresholdPercent = high.Value };

        var critical = EnvVariables.ReadInt("SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT");
        if (critical is not null)
            result = result with { CriticalPressureThresholdPercent = critical.Value };

        return result;
    }
}
