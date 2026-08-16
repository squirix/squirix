using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Golden snapshot for the gRPC service surface exposed by <c>MapSquirixServer</c>.</summary>
[Immutable]
public sealed class GrpcEndpointSurfaceGoldenSnapshotTests : ServerUnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the production gRPC service surface.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesProductionGrpcEndpointSurface()
    {
        var actual = new HashSet<string>(await GrpcEndpointSurfaceCollector.CollectProductionGrpcMethodsAsync(), StringComparer.OrdinalIgnoreCase);
        var path = NodePathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixGrpcEndpointSurface.golden.txt");
        Assert.True(File.Exists(path));

        var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var lines = await File.ReadAllLinesAsync(path, DefaultCancellationToken);
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].Length is 0)
                continue;

            _ = expected.Add(lines[i]);
        }

        if (actual.SetEquals(expected))
            return;

        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden gRPC endpoint surface mismatch. Update ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt if the change is intentional.");
        for (var i = 0; i < unexpected.Count; i++)
            _ = sb.Append("  + ").AppendLine(unexpected[i]);

        for (var i = 0; i < missing.Count; i++)
            _ = sb.Append("  - ").AppendLine(missing[i]);

        Assert.Fail(sb.ToString());
    }

    private static List<string> CollectSetDifference(HashSet<string> left, HashSet<string> right)
    {
        var result = new List<string>();
        foreach (var item in left)
        {
            if (!right.Contains(item))
                result.Add(item);
        }

        result.Sort(StringComparer.Ordinal);
        return result;
    }

    /// <summary>Collects gRPC service/method identities exposed by the production Squirix server mapping pipeline.</summary>
    private static class GrpcEndpointSurfaceCollector
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
            var builder = WebApplication.CreateBuilder(
                new WebApplicationOptions
                {
                    EnvironmentName = "Production",
                });

            _ = await builder.AddSquirixServerAsync(
                static options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
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

            methods.Sort(StringComparer.Ordinal);
            return methods;
        }
    }
}
