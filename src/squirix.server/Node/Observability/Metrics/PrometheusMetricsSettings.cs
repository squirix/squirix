namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Partial settings shape for <c>Squirix.settings.json</c> <c>PrometheusMetrics</c> section.
/// </summary>
internal sealed class PrometheusMetricsSettings
{
    public bool? Enabled { get; init; }

    public string? Path { get; init; }

    public bool? RequireAuth { get; init; }

    /// <summary>
    /// Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).
    /// </summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    public PrometheusMetricsEndpointOptions MergeInto(PrometheusMetricsEndpointOptions baseline) => new()
    {
        Enabled = Enabled ?? baseline.Enabled,
        Path = string.IsNullOrWhiteSpace(Path) ? baseline.Path : Path,
        RequireAuth = RequireAuth ?? baseline.RequireAuth,
    };
}
