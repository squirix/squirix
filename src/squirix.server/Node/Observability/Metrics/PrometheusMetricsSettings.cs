using System.Text.Json.Serialization;
using JetBrains.Annotations;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Partial settings shape for <c>Squirix.settings.json</c> <c>PrometheusMetrics</c> section.
/// </summary>
[UsedImplicitly]
internal sealed class PrometheusMetricsSettings
{
    [UsedImplicitly]
    [JsonPropertyName("path")]
    internal string? Path { get; init; }

    [UsedImplicitly]
    [JsonPropertyName("enabled")]
    internal bool? Enabled { get; init; }

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
