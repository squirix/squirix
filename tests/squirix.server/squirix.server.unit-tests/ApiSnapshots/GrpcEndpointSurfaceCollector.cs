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

/// <summary>Collects gRPC service/method identities exposed by the production Squirix server mapping pipeline.</summary>
internal static class GrpcEndpointSurfaceCollector
{
    /// <summary>
    /// Builds a production-like host and returns sorted gRPC method identities (<c>ServiceName/MethodName</c>).
    /// </summary>
    /// <returns>Sorted gRPC method identities for the mapped server surface.</returns>
    internal static async Task<List<string>> CollectProductionGrpcMethodsAsync()
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
            options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            loadDiscoveredSettings: false,
            cancellationToken: CancellationToken.None);

        return builder.Build();
    }

    private static List<string> CollectGrpcMethods(WebApplication app)
    {
        if (app is not IEndpointRouteBuilder routeBuilder)
            throw new InvalidOperationException("Web application does not expose endpoint data sources.");

        var methods = new List<string>();
        foreach (var source in routeBuilder.DataSources)
        {
            for (var index = 0; index < source.Endpoints.Count; index++)
            {
                var endpoint = source.Endpoints[index];
                var grpc = endpoint.Metadata.GetMetadata<GrpcMethodMetadata>();
                if (grpc is null)
                    continue;

                if (grpc.Method.Name.Contains("grpcunimplemented", StringComparison.Ordinal))
                    continue;

                methods.Add($"{grpc.Method.ServiceName}/{grpc.Method.Name}");
            }
        }

        methods.Sort(StringComparer.Ordinal);
        return methods;
    }
}
