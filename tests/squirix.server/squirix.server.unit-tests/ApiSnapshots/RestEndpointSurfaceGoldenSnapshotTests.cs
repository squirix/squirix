using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>
/// Golden snapshot for the REST endpoint surface exposed by <c>MapSquirixServer</c>.
/// </summary>
public sealed class RestEndpointSurfaceGoldenSnapshotTests : ServerUnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the production REST route surface.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesProductionRestEndpointSurface()
    {
        var actual = new HashSet<string>(await RestEndpointSurfaceCollector.CollectProductionRestRoutesAsync(), StringComparer.Ordinal);
        var path = NodePathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixRestEndpointSurface.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        var lines = await File.ReadAllLinesAsync(path, DefaultCancellationToken);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length is 0)
                continue;

            _ = expected.Add(line);
        }

        if (actual.SetEquals(expected))
            return;

        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);

        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden REST endpoint surface mismatch. Update ApiSnapshots/SquirixRestEndpointSurface.golden.txt if the change is intentional.");
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

    /// <summary>Collects HTTP route patterns exposed by the production Squirix server mapping pipeline.</summary>
    private static class RestEndpointSurfaceCollector
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
                options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
                loadDiscoveredSettings: false,
                cancellationToken: CancellationToken.None);

            return builder.Build();
        }

        /// <summary>
        /// Collects REST route identities (<c>METHOD /pattern</c>) from the host endpoint data sources,
        /// excluding gRPC methods and unimplemented placeholders.
        /// </summary>
        /// <param name="app">Built web application exposing endpoint route data.</param>
        /// <returns>Sorted list of REST route identities for golden comparison.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="app" /> does not expose endpoint data sources.</exception>
        private static List<string> CollectRestRoutes(WebApplication app)
        {
            if (app is not IEndpointRouteBuilder routeBuilder)
                throw new InvalidOperationException("Web application does not expose endpoint data sources.");

            var routes = new List<string>();
            foreach (var source in routeBuilder.DataSources)
            {
                for (var index = 0; index < source.Endpoints.Count; index++)
                    AppendRouteEndpoint(source.Endpoints[index], routes);
            }

            routes.Sort(StringComparer.Ordinal);
            return routes;
        }

        private static void AppendHttpMethods(RouteEndpoint route, string pattern, List<string> routes)
        {
            var methods = route.Metadata.GetMetadata<HttpMethodMetadata>();
            if (methods is null || methods.HttpMethods.Count is 0)
            {
                // Health probes often omit explicit HttpMethodMetadata; treat them as GET for the golden.
                if (pattern.StartsWith("/health", StringComparison.Ordinal))
                    routes.Add($"GET {pattern}");

                return;
            }

            var httpMethods = new List<string>(methods.HttpMethods);
            httpMethods.Sort(StringComparer.Ordinal);
            for (var methodIndex = 0; methodIndex < httpMethods.Count; methodIndex++)
                routes.Add($"{httpMethods[methodIndex]} {pattern}");
        }

        private static void AppendRouteEndpoint(Endpoint endpoint, List<string> routes)
        {
            if (endpoint is not RouteEndpoint route)
                return;

            // gRPC endpoints are covered by a separate contract surface and must not dilute REST goldens.
            if (route.Metadata.GetMetadata<GrpcMethodMetadata>() is not null)
                return;

            var pattern = route.RoutePattern.RawText ?? "/";
            if (pattern.Contains("grpcunimplemented", StringComparison.Ordinal))
                return;

            AppendHttpMethods(route, pattern, routes);
        }
    }
}
