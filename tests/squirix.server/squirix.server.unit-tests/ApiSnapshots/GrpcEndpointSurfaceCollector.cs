using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Collects gRPC service/method identities exposed by the production Squirix server mapping pipeline.</summary>
internal static class GrpcEndpointSurfaceCollector
{
    /// <summary>
    /// Builds a production-like host and returns sorted gRPC method identities (<c>ServiceName/MethodName</c>).
    /// </summary>
    /// <returns>Sorted gRPC method identities for the mapped server surface.</returns>
    internal static async Task<string[]> CollectProductionGrpcMethodsAsync()
    {
        await using var app = await BuildProductionHostAsync();
        _ = app.MapSquirixServer();
        return CollectGrpcMethods(app);
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

    private static string[] CollectGrpcMethods(WebApplication app)
    {
        if (app is not IEndpointRouteBuilder routeBuilder)
            throw new InvalidOperationException("Web application does not expose endpoint data sources.");

        var methods = new List<string>();
        var endpoints = routeBuilder.DataSources.SelectMany(static source => source.Endpoints);
        foreach (var endpoint in endpoints)
        {
            var grpc = endpoint.Metadata.GetMetadata<GrpcMethodMetadata>();
            if (grpc is null)
                continue;

            if (grpc.Method.Name.Contains("grpcunimplemented", StringComparison.Ordinal))
                continue;

            methods.Add($"{grpc.Method.ServiceName}/{grpc.Method.Name}");
        }

        return [.. methods.Order(StringComparer.Ordinal)];
    }
}
