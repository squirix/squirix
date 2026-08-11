using System.IO;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Loads unresolved memory pressure settings from <c>Squirix.settings.json</c> and environment variables.
/// </summary>
internal static class PressureBootstrap
{
    /// <summary>Loads memory pressure settings using the same settings file discovery as cluster bootstrap, then applies environment overrides.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Loaded settings before RAM budget resolution.</returns>
    internal static async Task<UnresolvedMemoryPressureOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = new UnresolvedMemoryPressureOptions();
        var (_, fileMerged) = await TryMergeFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return ApplyEnvironment(fileMerged);
    }

    /// <summary>Merges <c>MemoryPressure</c> from a specific settings file path.</summary>
    /// <param name="path">Full path to a JSON settings file.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the file exists and defines a <c>MemoryPressure</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    internal static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeFromSettingsFilePathAsync(
        string path,
        UnresolvedMemoryPressureOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path))
            return (false, baseline);

        return await SettingsJson.WithSquirixRootAsync(
            path,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("MemoryPressure", out var memoryPressure))
                    return (false, baseline);

                var section = SerializerProvider.Instance.Deserialize<PressureSettings>(memoryPressure.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Merges the <c>MemoryPressure</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>MemoryPressure</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    private static async Task<(bool Found, UnresolvedMemoryPressureOptions Merged)> TryMergeFromFileAsync(
        UnresolvedMemoryPressureOptions baseline,
        CancellationToken cancellationToken = default)
    {
        var path = SettingsJson.FindSettingsPath();
        return path is null ? (false, baseline) : await TryMergeFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    private sealed class PressureSettings
    {
        [JsonInclude]
        [JsonPropertyName("criticalPressureThresholdPercent")]
        private int? CriticalPressureThresholdPercent { get; init; }

        [JsonInclude]
        [JsonPropertyName("highPressureThresholdPercent")]
        private int? HighPressureThresholdPercent { get; init; }

        [JsonInclude]
        [JsonPropertyName("maxEstimatedCacheBytes")]
        private long? MaxEstimatedCacheBytes { get; init; }

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
}
