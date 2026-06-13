using System;
using System.Collections.Generic;
using System.Linq;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Collects gRPC service/method identities exposed by the production Squirix server mapping pipeline.
/// </summary>
internal static class GrpcEndpointSurfaceCollector
{
    /// <summary>
    /// Builds a production-like host and returns sorted gRPC method identities (<c>ServiceName/MethodName</c>).
    /// </summary>
    /// <returns>Sorted gRPC method identities for the mapped server surface.</returns>
    internal static string[] CollectProductionGrpcMethods()
    {
        using var app = BuildProductionHost();
        _ = app.MapSquirixServer();
        return CollectGrpcMethods(app);
    }

    private static WebApplication BuildProductionHost()
    {
        using var allocator = new PortAllocator(32000, 32999);
        var port = allocator.Allocate();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Production",
            });

        _ = builder.AddSquirixServer(options => options.Url = new Uri($"https://localhost:{port}"), loadDiscoveredSettings: false);

        return builder.Build();
    }

    private static string[] CollectGrpcMethods(WebApplication app)
    {
        var methods = new List<string>();
        var endpoints = ((IEndpointRouteBuilder)app).DataSources.SelectMany(static source => source.Endpoints);
        foreach (var endpoint in endpoints)
        {
            var grpc = endpoint.Metadata.GetMetadata<GrpcMethodMetadata>();
            if (grpc is null)
                continue;

            if (grpc.Method.Name.Contains("grpcunimplemented", StringComparison.Ordinal))
                continue;

            methods.Add($"{grpc.Method.ServiceName}/{grpc.Method.Name}");
        }

        return [.. methods.OrderBy(static method => method, StringComparer.Ordinal)];
    }
}
