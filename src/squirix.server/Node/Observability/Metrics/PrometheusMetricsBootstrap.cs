using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;

namespace Squirix.Server.Node.Observability.Metrics;

internal static class PrometheusMetricsBootstrap
{
    public static async Task<PrometheusMetricsEndpointOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = Default();
        var (found, merged) = await UnifiedSettings.TryMergePrometheusMetricsFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return found ? merged : baseline;
    }

    private static PrometheusMetricsEndpointOptions Default() => new();
}
