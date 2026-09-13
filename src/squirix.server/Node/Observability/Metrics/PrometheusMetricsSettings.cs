using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>JSON-serializable Prometheus metrics settings section.</summary>
[Immutable]
internal sealed class PrometheusMetricsSettings
{
    [JsonInclude]
    [JsonPropertyName("enabled")]
    internal bool? Enabled { get; init; }

    [JsonInclude]
    [JsonPropertyName("path")]
    internal string? Path { get; init; }

    /// <summary>Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).</summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    internal PrometheusMetricsEndpointOptions MergeInto(PrometheusMetricsEndpointOptions baseline) => new()
    {
        Enabled = Enabled ?? baseline.Enabled,
        Path = string.IsNullOrWhiteSpace(Path) ? baseline.Path : Path,
    };
}
