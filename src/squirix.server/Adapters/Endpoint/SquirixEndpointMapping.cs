using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability.Metrics;

namespace Squirix.Server.Adapters.Endpoint;

internal static class SquirixEndpointMapping
{
    internal static WebApplication MapSquirixEndpoints(this WebApplication app, bool authEnabled)
    {
        app.MapHealthEndpoints();

        var metricsOptions = app.Services.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;
        if (metricsOptions.Enabled)
            app.MapSquirixMetrics(metricsOptions.Path);

        var mtlsOptions = app.Services.GetRequiredService<MtlsOptions>();
        var mtlsMaterial = app.Services.GetRequiredService<MtlsCertificateMaterial>();
        var cacheGrpc = app.MapGrpcService<SquirixServiceAdapter<object?>>();
        if (authEnabled)
            _ = cacheGrpc.RequireAuthorization(SquirixAuthorizationPolicies.JwtBearer);

        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
            return app;
        _ = app.MapGrpcService<SquirixServiceAdapter<object?>>().RequireHost($"*:{mtlsOptions.InternalListenPort.ToString(CultureInfo.InvariantCulture)}").AllowAnonymous();
        return app;
    }
}
