using Squirix.Server.Node.Bootstrap;

namespace Squirix.Server.Node.Observability.Metrics;

internal static class PrometheusMetricsBootstrap
{
    public static PrometheusMetricsEndpointOptions Load() => UnifiedSettings.TryMergePrometheusMetricsFromFile(Default(), out var merged) ? merged : Default();

    private static PrometheusMetricsEndpointOptions Default() => new();
}
