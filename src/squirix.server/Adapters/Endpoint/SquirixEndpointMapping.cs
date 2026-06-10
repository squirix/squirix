using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Node.Observability.Metrics;

namespace Squirix.Server.Adapters.Endpoint;

internal static class SquirixEndpointMapping
{
    public static WebApplication MapSquirixEndpoints(this WebApplication app, bool authEnabled)
    {
        app.MapHealthEndpoints();

        var metricsOptions = app.Services.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;
        if (metricsOptions.Enabled)
            app.MapSquirixMetrics(metricsOptions.Path);

        app.MapAdminEndpoints(app.Environment, authEnabled);

        var cacheGrpc = app.MapGrpcService<SquirixServiceAdapter<object?>>();
        if (authEnabled)
            _ = cacheGrpc.RequireAuthorization("ApiOrJwt");

        app.MapCacheEndpoints<object?>("/cache", authEnabled);
        return app;
    }
}
