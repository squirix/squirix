using System.IO;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability.Metrics;

internal static class PrometheusMetricsBootstrap
{
    internal static async Task<PrometheusMetricsEndpointOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = Default();
        var (found, merged) = await TryMergeFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return found ? merged : baseline;
    }

    /// <summary>Merges <c>PrometheusMetrics</c> from a specific settings file path.</summary>
    /// <param name="file">Full path to a JSON settings file.</param>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the file exists and defines a <c>PrometheusMetrics</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    internal static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergeFromSettingsFilePathAsync(
        string file,
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(file))
            return (false, baseline);

        return await SettingsJson.WithSquirixRootAsync(
            file,
            baseline,
            static (root, baseline) =>
            {
                if (!root.TryGetProperty("PrometheusMetrics", out var prometheusMetrics))
                    return (false, baseline);

                var section = SerializationProvider.Instance.Deserialize<PrometheusMetricsSettings>(prometheusMetrics.GetRawText());
                var merged = section is null ? baseline : section.MergeInto(baseline);
                return (true, merged);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static PrometheusMetricsEndpointOptions Default() => new();

    /// <summary>
    /// Merges the <c>PrometheusMetrics</c> JSON section onto <paramref name="baseline" /> when the settings file exists and contains that section.
    /// </summary>
    /// <param name="baseline">Baseline options when the section is absent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is <see langword="true" /> when the settings file exists and defines a <c>PrometheusMetrics</c> object,
    /// and <c>Merged</c> is the merged result.
    /// </returns>
    private static async Task<(bool Found, PrometheusMetricsEndpointOptions Merged)> TryMergeFromFileAsync(
        PrometheusMetricsEndpointOptions baseline,
        CancellationToken cancellationToken = default)
    {
        var path = SettingsJson.FindSettingsPath();
        return path is null ? (false, baseline) : await TryMergeFromSettingsFilePathAsync(path, baseline, cancellationToken).ConfigureAwait(false);
    }

    private sealed class PrometheusMetricsSettings
    {
        [JsonInclude]
        [JsonPropertyName("enabled")]
        private bool? Enabled { get; init; }

        [JsonInclude]
        [JsonPropertyName("path")]
        private string? Path { get; init; }

        /// <summary>
        /// Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).
        /// </summary>
        /// <param name="baseline">Baseline options.</param>
        /// <returns>Merged options.</returns>
        internal PrometheusMetricsEndpointOptions MergeInto(PrometheusMetricsEndpointOptions baseline) => new()
        {
            Enabled = Enabled ?? baseline.Enabled,
            Path = string.IsNullOrWhiteSpace(Path) ? baseline.Path : Path,
        };
    }
}
