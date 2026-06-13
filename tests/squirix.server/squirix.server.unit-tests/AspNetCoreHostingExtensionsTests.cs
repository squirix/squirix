using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Verifies the public ASP.NET Core custom-hosting entry point.
/// </summary>
public sealed class AspNetCoreHostingExtensionsTests
{
    /// <summary>
    /// Ensures a custom ASP.NET Core application can register, map, and start a standalone Squirix node.
    /// </summary>
    /// <returns>A task that completes when the custom host has started and stopped.</returns>
    [Fact]
    public async Task CustomAspNetCoreHostCanStartMappedSquirixServer()
    {
        using var allocator = new PortAllocator(26000, 26999);
        var port = allocator.Allocate();
        var url = $"https://localhost:{port}";
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = builder.AddSquirixServer(
            options =>
            {
                options.NodeId = "aspnet-test";
                options.Url = new Uri(url);
            },
            loadDiscoveredSettings: false);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        var endpoints = ((IEndpointRouteBuilder)app).DataSources.SelectMany(static source => source.Endpoints).ToArray();
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("gRPC", StringComparison.OrdinalIgnoreCase) == true);
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("/health", StringComparison.OrdinalIgnoreCase) == true);

        await app.StartAsync(TestContext.Current.CancellationToken);
        await app.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Ensures a configured data directory keeps the server's default strict fsync persistence mode.
    /// </summary>
    [Fact]
    public void DataDirectoryOverridePreservesStrictFsyncDefault()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var dataDir = PathKit.Combine(Path.GetTempPath(), "squirix-aspnet-tests", Guid.NewGuid().ToString("N"));
        using var allocator = new PortAllocator(25000, 25999);
        var port = allocator.Allocate();

        _ = builder.AddSquirixServer(
            options =>
            {
                options.Url = new Uri($"https://localhost:{port}");
                options.UsePersistence(dataDir);
            },
            loadDiscoveredSettings: false);

        using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();

        Assert.Equal(dataDir, persistence.DataDir);

        if (Directory.Exists(dataDir))
            Directory.Delete(dataDir, true);
    }

    /// <summary>
    /// Ensures package extensions can decorate the hosted basic cache pipeline without internal server contracts.
    /// </summary>
    [Fact]
    public void PackageExtensionCanDecorateBasicCachePipeline()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var callbackCount = 0;
        using var allocator = new PortAllocator(28000, 28999);
        var port = allocator.Allocate();

        _ = builder.AddSquirixServer(
            options => options.Url = new Uri($"https://localhost:{port}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions =>
            {
                extensions.DecorateCachePipeline = (_, pipeline) =>
                {
                    callbackCount++;
                    return pipeline;
                };
            });

        using (var app = builder.Build())
            _ = app.Services.GetRequiredService<ICacheRuntime>();

        Assert.Equal(1, callbackCount);
    }

    /// <summary>
    /// Ensures optional package extensions can register services and map endpoints through the public hosting API.
    /// </summary>
    [Fact]
    public void PackageExtensionCanRegisterServiceAndMapEndpoint()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var marker = new ExtensionMarker();
        using var allocator = new PortAllocator(27000, 27999);
        var port = allocator.Allocate();

        _ = builder.AddSquirixServer(
            options => options.Url = new Uri($"https://localhost:{port}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions =>
            {
                extensions.ConfigureServices = services => services.AddSingleton(marker);
                extensions.MapEndpoints = static app => app.MapGet("/extension-test", static () => "ok");
            });

        using var app = builder.Build();
        _ = app.MapSquirixServer();

        Assert.Same(marker, app.Services.GetRequiredService<ExtensionMarker>());
        var endpoints = ((IEndpointRouteBuilder)app).DataSources.SelectMany(static source => source.Endpoints).ToArray();
        Assert.Contains(endpoints, static endpoint => endpoint.DisplayName?.Contains("/extension-test", StringComparison.Ordinal) == true);
    }

    /// <summary>
    /// Ensures package extensions receive the host authentication state while mapping protocol endpoints.
    /// </summary>
    [Fact]
    public void PackageExtensionReceivesAuthenticationStateWhileMappingEndpoints()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        bool? authEnabled = null;
        using var allocator = new PortAllocator(29000, 29999);
        var port = allocator.Allocate();

        _ = builder.AddSquirixServer(
            options => options.Url = new Uri($"https://localhost:{port}"),
            loadDiscoveredSettings: false,
            configureExtensions: extensions => extensions.MapEndpointsWithAuthorization = (_, enabled) => authEnabled = enabled);

        using var app = builder.Build();
        _ = app.MapSquirixServer();

        Assert.False(authEnabled);
    }

    private sealed class ExtensionMarker;
}
