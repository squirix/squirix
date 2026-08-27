namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Options for the built-in Prometheus-compatible metrics HTTP endpoint on the server host.</summary>
internal sealed class PrometheusMetricsEndpointOptions
{
    /// <summary>Gets or sets a value indicating whether the metrics endpoint is mapped on the server Kestrel host.</summary>
    internal bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the request path for metrics scraping. Defaults to <c language="csharp">/metrics</c>.
    /// </summary>
    internal string Path { get; set; } = "/metrics";
}
