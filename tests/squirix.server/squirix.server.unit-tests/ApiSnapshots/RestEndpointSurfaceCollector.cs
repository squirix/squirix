using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Collects HTTP route patterns exposed by the production Squirix server mapping pipeline.</summary>
internal static class RestEndpointSurfaceCollector
{
    /// <summary>Builds a production-like host and returns sorted REST route identities (method + path).</summary>
    /// <returns>Sorted REST route identities for the mapped server surface.</returns>
    internal static async Task<List<string>> CollectProductionRestRoutesAsync()
    {
        await using var app = await BuildProductionHostAsync();
        _ = app.MapSquirixServer();
        return CollectRestRoutes(app);
    }

    private static async Task<WebApplication> BuildProductionHostAsync()
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Production",
            });

        _ = await builder.AddSquirixServerAsync(
            options => options.Url = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            loadDiscoveredSettings: false,
            cancellationToken: CancellationToken.None);

        return builder.Build();
    }

    private static List<string> CollectRestRoutes(WebApplication app)
    {
        if (app is not IEndpointRouteBuilder routeBuilder)
            throw new InvalidOperationException("Web application does not expose endpoint data sources.");

        var routes = new List<string>();
        foreach (var source in routeBuilder.DataSources)
        {
            foreach (var endpoint in source.Endpoints)
            {
                if (endpoint is not RouteEndpoint route)
                    continue;

                if (route.Metadata.GetMetadata<GrpcMethodMetadata>() is not null)
                    continue;

                var pattern = route.RoutePattern.RawText ?? "/";
                if (pattern.Contains("grpcunimplemented", StringComparison.Ordinal))
                    continue;

                var methods = route.Metadata.GetMetadata<HttpMethodMetadata>();
                if (methods is null || methods.HttpMethods.Count is 0)
                {
                    if (pattern.StartsWith("/health", StringComparison.Ordinal))
                        routes.Add($"GET {pattern}");

                    continue;
                }

                var httpMethods = new List<string>(methods.HttpMethods);
                httpMethods.Sort(StringComparer.Ordinal);
                foreach (var method in httpMethods)
                    routes.Add($"{method} {pattern}");
            }
        }

        routes.Sort(StringComparer.Ordinal);
        return routes;
    }
}
