using System;
using System.Collections.Generic;
using System.Linq;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Collects HTTP route patterns exposed by the production Squirix server mapping pipeline.
/// </summary>
internal static class RestEndpointSurfaceCollector
{
    /// <summary>
    /// Builds a production-like host and returns sorted REST route identities (method + path).
    /// </summary>
    /// <returns>Sorted REST route identities for the mapped server surface.</returns>
    internal static string[] CollectProductionRestRoutes()
    {
        using var app = BuildProductionHost();
        _ = app.MapSquirixServer();
        return CollectRestRoutes(app);
    }

    private static WebApplication BuildProductionHost()
    {
        using var allocator = new PortAllocator(31000, 31999);
        var port = allocator.Allocate();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Production",
            });

        _ = builder.AddSquirixServer(
            options => options.Url = new Uri($"https://localhost:{port}"),
            loadDiscoveredSettings: false);

        return builder.Build();
    }

    private static string[] CollectRestRoutes(WebApplication app)
    {
        var routes = new List<string>();
        var endpoints = ((IEndpointRouteBuilder)app).DataSources.SelectMany(static source => source.Endpoints);
        foreach (var endpoint in endpoints)
        {
            if (endpoint is not RouteEndpoint route)
                continue;

            if (route.Metadata.GetMetadata<GrpcMethodMetadata>() is not null)
                continue;

            var pattern = route.RoutePattern.RawText ?? "/";
            if (pattern.Contains("grpcunimplemented", StringComparison.Ordinal))
                continue;

            var methods = route.Metadata.GetMetadata<HttpMethodMetadata>();
            if (methods is null || methods.HttpMethods.Count == 0)
            {
                if (pattern.StartsWith("/health", StringComparison.Ordinal))
                    routes.Add($"GET {pattern}");

                continue;
            }

            foreach (var method in methods.HttpMethods.OrderBy(static httpMethod => httpMethod, StringComparer.Ordinal))
                routes.Add($"{method} {pattern}");
        }

        return [.. routes.OrderBy(static route => route, StringComparer.Ordinal)];
    }
}
