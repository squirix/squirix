using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the public ASP.NET Core custom-hosting entry point.</summary>
public sealed class AspNetCoreHostingExtensionsTests : ServerUnitTestBase
{
    private static readonly SocketsHttpHandler LoopbackHandler = LoopbackHttp.CreateHandler();
    private static readonly HttpClient LoopbackClient = new(LoopbackHandler, false);

    /// <summary>Ensures a custom ASP.NET Core application can register, map, and start a standalone Squirix node.</summary>
    [Fact]
    public async Task CustomAspNetCoreHostCanStartMappedSquirixServer()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = await builder.AddSquirixServerAsync(
            static options =>
            {
                options.NodeId = "aspnet-test";
                options.Uri = new Uri(InvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort()));
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
        var optionsConfigurer = new PersistenceOptionsConfigurer(port, dir.Path);

        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Url = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}");
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
        var state = new DecoratePipelineState();
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var optionsConfigurer = new UriOptionsConfigurer(port);
        var extensionsConfigurer = new DecoratePipelineExtensionsConfigurer(state);

        _ = await builder.AddSquirixServerAsync(
            optionsConfigurer.Apply,
            loadDiscoveredSettings: false,
            configureExtensions: extensionsConfigurer.Apply,
            cancellationToken: DefaultCancellationToken);

        await using (var app = builder.Build())
            _ = app.Services.GetRequiredService<ICacheRuntime>();

        Assert.Equal(1, state.CallbackCount);
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
        var extensionsConfigurer = new MarkerExtensionsConfigurer(marker);

        _ = await builder.AddSquirixServerAsync(
            static options => options.Uri = new Uri(InvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            configureExtensions: extensionsConfigurer.Apply,
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
    public async Task PackageExtensionReceivesStateMappingEndpoints()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var state = new AuthorizationStateCapture();
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var optionsConfigurer = new UriOptionsConfigurer(port);
        var extensionsConfigurer = new AuthorizationStateExtensionsConfigurer(state);

        _ = await builder.AddSquirixServerAsync(
            optionsConfigurer.Apply,
            loadDiscoveredSettings: false,
            configureExtensions: extensionsConfigurer.Apply,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        Assert.False(state.AuthEnabled);
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

    private sealed class AuthorizationStateCapture
    {
        internal bool? AuthEnabled { get; set; }
    }

    private sealed class AuthorizationStateExtensionsConfigurer
    {
        private readonly AuthorizationStateCapture _state;

        internal AuthorizationStateExtensionsConfigurer(AuthorizationStateCapture state)
        {
            _state = state;
            Apply = ApplyCore;
        }

        internal Action<ExtensionOptions> Apply { get; }

        private void ApplyCore(ExtensionOptions extensions) => extensions.MapEndpointsWithAuthorization = CaptureAuthorizationState;

        private void CaptureAuthorizationState(WebApplication application, bool enabled)
        {
            _ = application;
            _state.AuthEnabled = enabled;
        }
    }

    private sealed class DecoratePipelineExtensionsConfigurer
    {
        private readonly DecoratePipelineState _state;

        internal DecoratePipelineExtensionsConfigurer(DecoratePipelineState state)
        {
            _state = state;
            Apply = ApplyCore;
        }

        internal Action<ExtensionOptions> Apply { get; }

        private void ApplyCore(ExtensionOptions extensions) => extensions.DecorateCachePipeline = Decorate;

        private ISquirixServerCachePipeline Decorate(IServiceProvider services, ISquirixServerCachePipeline pipeline)
        {
            _ = services;
            _state.CallbackCount++;
            return pipeline;
        }
    }

    private sealed class DecoratePipelineState
    {
        internal int CallbackCount { get; set; }
    }

    private sealed class FixedUriOptionsConfigurer
    {
        private readonly Uri _uri;

        internal FixedUriOptionsConfigurer(Uri uri)
        {
            _uri = uri;
            Apply = ApplyCore;
        }

        internal Action<SquirixServerOptions> Apply { get; }

        private void ApplyCore(SquirixServerOptions options) => options.Uri = _uri;
    }

    private sealed class MarkerExtensionsConfigurer
    {
        private readonly ExtensionMarker _marker;

        internal MarkerExtensionsConfigurer(ExtensionMarker marker)
        {
            _marker = marker;
            Apply = ApplyCore;
        }

        internal Action<ExtensionOptions> Apply { get; }

        private void ApplyCore(ExtensionOptions extensions)
        {
            extensions.ConfigureServices = ConfigureServices;
            extensions.MapEndpoints = static app => app.MapGet("/extension-test", static () => "ok");
        }

        private void ConfigureServices(IServiceCollection services) => services.AddSingleton(_marker);
    }

    private sealed class PersistenceOptionsConfigurer
    {
        private readonly string _dataDirectory;
        private readonly int _port;

        internal PersistenceOptionsConfigurer(int port, string dataDirectory)
        {
            _port = port;
            _dataDirectory = dataDirectory;
            Apply = ApplyCore;
        }

        internal Action<SquirixServerOptions> Apply { get; }

        private void ApplyCore(SquirixServerOptions options)
        {
            options.Uri = new Uri(InvariantIndexStrings.FormatHttpsOrigin("localhost", _port));
            options.UsePersistence(_dataDirectory);
        }
    }

    private sealed class UriOptionsConfigurer
    {
        private readonly int _port;

        internal UriOptionsConfigurer(int port)
        {
            _port = port;
            Apply = ApplyCore;
        }

        internal Action<SquirixServerOptions> Apply { get; }

        private void ApplyCore(SquirixServerOptions options) => options.Uri = new Uri(InvariantIndexStrings.FormatHttpsOrigin("localhost", _port));
    }
}
