using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Maps the built-in Prometheus-compatible metrics endpoint for the squirix server host.</summary>
internal static class EndpointExtensions
{
    /// <summary>
    /// Maps a lightweight Prometheus-compatible metrics endpoint that scrapes <see cref="System.Diagnostics.Metrics" />
    /// instruments from the <c language="csharp">Squirix</c> meter.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="path">The request path to expose metrics on. Defaults to <c language="csharp">/metrics</c>.</param>
    /// <param name="scraper">The metrics scraper for the server's meter.</param>
    internal static void MapSquirixMetrics(this IEndpointRouteBuilder endpoints, string path, PrometheusMetricsScraper scraper)
    {
        _ = endpoints.MapGet(
            path,
            async ctx =>
            {
                if (!await ConnectionSecurity.IsRequestAuthorizedAsync(ctx).ConfigureAwait(false))
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                ctx.Response.ContentType = "text/plain; version=0.0.4";
                var text = scraper.Scrape();
                await ctx.Response.WriteAsync(text, ctx.RequestAborted).ConfigureAwait(false);
            });
    }
}
