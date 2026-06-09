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
    /// <param name="requireAuth">When <see langword="true" />, requires API/JWT authorization on the endpoint.</param>
    internal static void MapSquirixMetrics(this IEndpointRouteBuilder endpoints, string path = "/metrics", bool requireAuth = false)
    {
        _ = PrometheusMetricsScraper.Instance;

        var route = endpoints.MapGet(
            path,
            static async ctx =>
            {
                ctx.Response.ContentType = "text/plain; version=0.0.4";
                var text = PrometheusMetricsScraper.Instance.Scrape();
                await ctx.Response.WriteAsync(text);
            });

        if (requireAuth)
            _ = route.RequireAuthorization("ApiOrJwt");
    }
}
