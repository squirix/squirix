using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Maps the built-in Prometheus-compatible metrics endpoint for the squirix server host.
/// </summary>
internal static class SquirixMetricsEndpointExtensions
{
    /// <summary>
    /// Maps a lightweight Prometheus-compatible metrics endpoint that scrapes <see cref="System.Diagnostics.Metrics" />
    /// instruments from the <c>Squirix</c> meter.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="path">The request path to expose metrics on. Defaults to <c>/metrics</c>.</param>
    /// <param name="accessMode">Loopback-or-authenticated by default when <see cref="PrometheusMetricsEndpointOptions.RequireAuth" /> is enabled.</param>
    internal static void MapSquirixMetrics(this IEndpointRouteBuilder endpoints, string path = "/metrics", MetricsEndpointAccessMode accessMode = MetricsEndpointAccessMode.Anonymous)
    {
        _ = PrometheusMetricsScraper.Instance;

        _ = endpoints.MapGet(
            path,
            async ctx =>
            {
                if (!SquirixMetricsConnectionSecurity.IsRequestAuthorized(ctx, accessMode))
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                ctx.Response.ContentType = "text/plain; version=0.0.4";
                var text = PrometheusMetricsScraper.Instance.Scrape();
                await ctx.Response.WriteAsync(text);
            });
    }
}
