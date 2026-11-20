using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Diagnostics;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixEndpointMapping
{
    internal static WebApplication MapSquirixEndpoints(this WebApplication app)
    {
        MapHealthEndpoints(app);

        var metricsOptions = app.Services.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;
        if (metricsOptions.Enabled && app.Services.GetService<PrometheusMetricsScraper>() is { } scraper)
            app.MapSquirixMetrics(metricsOptions.Path, scraper);

        var mtlsOptions = app.Services.GetRequiredService<MtlsOptions>();
        var mtlsMaterial = app.Services.GetRequiredService<MtlsCertificate>();
        var featureState = app.Services.GetRequiredService<FeatureState>();
        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
            return app;

        // The Host header is client-controlled, so the anonymous internal mappings are selected by the accepted connection
        // (listener port plus peer certificate) through InternalListenerMatcherPolicy, never by host filters.
        var internalListener = new InternalListenerEndpointMetadata(mtlsOptions.InternalListenPort, true);
        _ = app.MapPost(PeerProtocol.Prefix + "ping", PeerEndpoint.PingAsync)
            .WithMetadata(internalListener).AllowAnonymous();
        _ = app.MapPost(PeerProtocol.Prefix + "owner/{operation}", ForwardAsync).WithMetadata(internalListener).AllowAnonymous();

        // Closed replication service: internal listener only, for foundation transport tests
        // and on network-replication-activated hosts.
        if (featureState.FoundationOnly || featureState.NetworkReplicationEnabled)
            _ = app.MapPost(PeerProtocol.Prefix + "replication/{operation}", PeerDispatch.HandleAsync).WithMetadata(internalListener).AllowAnonymous();

        return app;
    }

    internal static WebApplication MapEndpoints(WebApplication app, bool authEnabled)
    {
        _ = app.MapSquirixEndpoints();
        var extensions = app.Services.GetService<ExtensionOptions>();
        extensions?.MapEndpoints?.Invoke(app);
        extensions?.MapEndpointsWithAuthorization?.Invoke(app, authEnabled);
        return app;
    }

    internal static HealthReadyDetailsResponse BuildReadyDetailsResponse(HealthReadyDetailsSnapshot snapshot) =>
        HealthDetailsResponseMapper.Build(snapshot);

    private static Task ForwardAsync(HttpContext http) => PeerEndpoint.HandleAsync(http, NativePeerEndpoint.ForwardAsync);

    private static void MapHealthEndpoints(IEndpointRouteBuilder app)
    {
        _ = app.MapHealthChecks(
            "/health/live",
            new HealthCheckOptions
            {
                Predicate = static registration => registration.Tags.Contains("live"),
            });
        _ = app.MapHealthChecks(
            "/health/ready",
            new HealthCheckOptions
            {
                Predicate = static registration => registration.Tags.Contains("ready"),
            });

        // Manual response writing (instead of Results.Ok): the MapGet overload taking Delegate
        // is trim-unsafe, while this RequestDelegate-shaped handler binds the AOT-clean overload.
        // The bytes below match Results.Ok("OK") exactly: 200 + application/json + "\"OK\"".
        _ = app.MapGet(
            "/health",
            static async ctx =>
            {
                ctx.Response.ContentType = "application/json; charset=utf-8";
                await ctx.Response.WriteAsync("\"OK\"", ctx.RequestAborted).ConfigureAwait(false);
            });
        MapReadyDetailsEndpoint(app);
    }

    private static void MapReadyDetailsEndpoint(IEndpointRouteBuilder app)
    {
        // RequestDelegate-shaped handler (instead of a Delegate with DI parameters): the MapGet
        // overload taking Delegate is trim-unsafe, while this shape binds the AOT-clean overload.
        // Services resolve explicitly and the DTO is written with source-generated metadata,
        // so the wire behavior matches the previous Results-based version exactly.
        _ = app.MapGet(
            "/health/ready/details",
            static async ctx =>
            {
                if (!await ConnectionSecurity.IsRequestAuthorizedAsync(ctx).ConfigureAwait(false))
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                var provider = ctx.RequestServices.GetRequiredService<IHealthReadyDetailsProvider>();
                var snapshot = await provider.GetSnapshotAsync(ctx.RequestAborted).ConfigureAwait(false);
                var details = BuildReadyDetailsResponse(snapshot);

                ctx.Response.ContentType = "application/json; charset=utf-8";
                await JsonSerializer.SerializeAsync(ctx.Response.Body, details, RestJsonSerializerContext.Default.HealthReadyDetailsResponse, ctx.RequestAborted)
                                    .ConfigureAwait(false);
            });
    }
}
