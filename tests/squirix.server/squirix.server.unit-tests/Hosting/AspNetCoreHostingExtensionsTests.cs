using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the public ASP.NET Core custom-hosting entry point.</summary>
[Immutable]
public sealed class AspNetCoreHostingExtensionsTests : IsolatedStorageTestBase
{
    private static readonly Action<WebApplication> MapJournalQuotaEndpoint = static app => app.MapGet(
        "/throw-journal-quota",
        static _ => throw new JournalCapacityExceededException());

    private static readonly Action<ExtensionOptions> ConfigureJournalQuotaExtensions = static extensions => extensions.MapEndpoints = MapJournalQuotaEndpoint;
    private static readonly SocketsHttpHandler LoopbackHandler = LoopbackHttp.CreateHandler();
    private static readonly HttpClient LoopbackClient = new(LoopbackHandler, false);

    private static readonly Action<WebApplication> MapExtensionTestEndpoint = static app => app.MapGet(
        "/extension-test",
        static context => context.Response.WriteAsync("ok", context.RequestAborted));

    /// <summary>Ensures a custom ASP.NET Core application can register, map, and start a standalone Squirix node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CustomAspNetCoreHostStartsMappedServer(CancellationToken cancellationToken)
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
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort()));
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        var endpoints = GetMappedEndpoints(app);
        _ = await Assert.That(endpoints).Contains(static endpoint => endpoint.DisplayName?.Contains("gRPC", StringComparison.OrdinalIgnoreCase) == true);
        _ = await Assert.That(endpoints).Contains(static endpoint => endpoint.DisplayName?.Contains("/health", StringComparison.OrdinalIgnoreCase) == true);

        await app.StartAsync(cancellationToken);
        await app.StopAsync(cancellationToken);
    }

    /// <summary>Ensures a configured data directory keeps the server's default strict fsync persistence mode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DataDirOverrideKeepsStrictFsyncDefault(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var optionsConfigurer = new PersistenceOptionsConfigurer(port, Dir.Path);

        _ = await builder.AddSquirixServerAsync(optionsConfigurer.Apply, loadDiscoveredSettings: false, cancellationToken: cancellationToken);

        await using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();

        _ = await Assert.That(persistence.DataDir).IsEqualTo(Dir.Path);
    }

    /// <summary>Ensures package extensions receive the host authentication state while mapping protocol endpoints.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExtensionReceivesStateWhenMappingRoutes(CancellationToken cancellationToken)
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
            cancellationToken: cancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        _ = await Assert.That(state.AuthEnabled).IsFalse();
    }

    /// <summary>Ensures optional package extensions can register services and map endpoints through the public hosting API.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExtensionRegistersServicesAndMapsRoutes(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var marker = new ExtensionMarker("extension-test");
        var extensionsConfigurer = new MarkerExtensionsConfigurer(marker);

        _ = await builder.AddSquirixServerAsync(
            static options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            configureExtensions: extensionsConfigurer.Apply,
            cancellationToken: cancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        var registeredMarker = app.Services.GetRequiredService<ExtensionMarker>();
        _ = await Assert.That(registeredMarker).IsSameReferenceAs(marker);
        _ = await Assert.That(registeredMarker.Name).IsEqualTo(marker.Name);
        var endpoints = GetMappedEndpoints(app);
        _ = await Assert.That(endpoints).Contains(static endpoint => endpoint.DisplayName?.Contains("/extension-test", StringComparison.Ordinal) == true);
    }

    /// <summary>Ensures MapSquirixServer middleware maps journal capacity to HTTP 429.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MapSquirixServerMapsQuotaToHttp429(CancellationToken cancellationToken)
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", port));
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });
        var optionsConfigurer = new FixedUriOptionsConfigurer(uri);

        _ = await builder.AddSquirixServerAsync(
            optionsConfigurer.Apply,
            loadDiscoveredSettings: false,
            configureExtensions: ConfigureJournalQuotaExtensions,
            cancellationToken: cancellationToken);

        await using var app = builder.Build();
        _ = app.MapSquirixServer();
        await app.StartAsync(cancellationToken);

        using var response = await LoopbackClient.GetAsync(new Uri(uri, "/throw-journal-quota"), cancellationToken);

        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);
        await app.StopAsync(cancellationToken);
    }

    /// <summary>Ensures package extensions can decorate the hosted basic cache pipeline without internal server contracts.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PackageExtensionDecoratesCachePipeline(CancellationToken cancellationToken)
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
            cancellationToken: cancellationToken);

        await using (var app = builder.Build())
            _ = app.Services.GetRequiredService<ICacheRuntime>();

        _ = await Assert.That(state.CallbackCount).IsEqualTo(1);
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

    [Immutable]
    private sealed record ExtensionMarker(string Name);

    private sealed class AuthorizationStateCapture
    {
        internal bool? AuthEnabled { get; set; }
    }

    [Immutable]
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

        private void CaptureAuthorizationState(WebApplication application, bool enabled) => _state.AuthEnabled = enabled;
    }

    [Immutable]
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
            _state.CallbackCount++;
            return pipeline;
        }
    }

    private sealed class DecoratePipelineState
    {
        internal int CallbackCount { get; set; }
    }

    [Immutable]
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

    [Immutable]
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
            extensions.MapEndpoints = MapExtensionTestEndpoint;
        }

        private void ConfigureServices(IServiceCollection services) => services.AddSingleton(_marker);
    }

    [Immutable]
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
            options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", _port));
            options.UsePersistence(_dataDirectory);
        }
    }

    [Immutable]
    private sealed class UriOptionsConfigurer
    {
        private readonly int _port;

        internal UriOptionsConfigurer(int port)
        {
            _port = port;
            Apply = ApplyCore;
        }

        internal Action<SquirixServerOptions> Apply { get; }

        private void ApplyCore(SquirixServerOptions options) => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", _port));
    }
}
