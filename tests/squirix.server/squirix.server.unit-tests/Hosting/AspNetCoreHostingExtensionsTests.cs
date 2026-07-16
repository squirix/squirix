using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the public ASP.NET Core custom-hosting entry point.</summary>
public sealed class AspNetCoreHostingExtensionsTests : ServerUnitTestBase
{
    /// <summary>Ensures a custom ASP.NET Core application can register, map, and start a standalone Squirix node.</summary>
    [Fact]
    public async Task CustomAspNetCoreHostCanStartMappedSquirixServer()
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.NodeId = "aspnet-test";
                options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}");
            },
            loadDiscoveredSettings: false,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        var endpoints = GetMappedEndpoints(app);
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("gRPC", StringComparison.OrdinalIgnoreCase) is true);
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("/health", StringComparison.OrdinalIgnoreCase) is true);

        await app.StartAsync(DefaultCancellationToken);
        await app.StopAsync(DefaultCancellationToken);
    }

    /// <summary>Ensures a configured data directory keeps the server's default strict fsync persistence mode.</summary>
    [Fact]
    public async Task DataDirectoryOverridePreservesStrictFsyncDefault()
    {
        using var dir = new TempDirectory("squirix-aspnet-tests");
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var port = ListenPortPool.ServerUnitTests.AllocatePort();

        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}");
                options.UsePersistence(dir);
            },
            loadDiscoveredSettings: false,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();

        Assert.Equal(dir.Path, persistence.DataDir);
    }

    /// <summary>Ensures package extensions can decorate the hosted basic cache pipeline without internal server contracts.</summary>
    [Fact]
    public async Task PackageExtensionCanDecorateBasicCachePipeline()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var callbackCount = 0;
        var port = ListenPortPool.ServerUnitTests.AllocatePort();

        _ = await builder.AddSquirixServerAsync(
            options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions =>
            {
                extensions.DecorateCachePipeline = (_, pipeline) =>
                {
                    callbackCount++;
                    return pipeline;
                };
            },
            cancellationToken: DefaultCancellationToken);

        await using (var app = builder.Build())
            _ = app.Services.GetRequiredService<ICacheRuntime>();

        Assert.Equal(1, callbackCount);
    }

    /// <summary>Ensures optional package extensions can register services and map endpoints through the public hosting API.</summary>
    [Fact]
    public async Task PackageExtensionCanRegisterServiceAndMapEndpoint()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var marker = new ExtensionMarker("extension-test");
        var port = ListenPortPool.ServerUnitTests.AllocatePort();

        _ = await builder.AddSquirixServerAsync(
            options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions =>
            {
                extensions.ConfigureServices = services => services.AddSingleton(marker);
                extensions.MapEndpoints = static app => app.MapGet("/extension-test", static () => "ok");
            },
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        var registeredMarker = app.Services.GetRequiredService<ExtensionMarker>();
        Assert.Same(marker, registeredMarker);
        Assert.Equal(marker.Name, registeredMarker.Name);
        var endpoints = GetMappedEndpoints(app);
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("/extension-test", StringComparison.Ordinal) is true);
    }

    /// <summary>Ensures package extensions receive the host authentication state while mapping protocol endpoints.</summary>
    [Fact]
    public async Task PackageExtensionReceivesAuthenticationStateWhileMappingEndpoints()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        bool? authEnabled = null;
        var port = ListenPortPool.ServerUnitTests.AllocatePort();

        _ = await builder.AddSquirixServerAsync(
            options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions => extensions.MapEndpointsWithAuthorization = (_, enabled) => authEnabled = enabled,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        Assert.False(authEnabled);
    }

    private static List<Endpoint> GetMappedEndpoints(WebApplication app)
    {
        if (app is not IEndpointRouteBuilder routeBuilder)
            throw new InvalidOperationException("Web application does not expose endpoint data sources.");

        var capacity = 0;
        foreach (var source in routeBuilder.DataSources)
            capacity += source.Endpoints.Count;

        var endpoints = new List<Endpoint>(capacity);
        foreach (var source in routeBuilder.DataSources)
            endpoints.AddRange(source.Endpoints);

        return endpoints;
    }

    private sealed record ExtensionMarker(string Name);
}
