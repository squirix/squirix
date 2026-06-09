using System;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixServerHostingComposition
{
    public static void ConfigureBuilder(WebApplicationBuilder builder, SquirixServerOptions options, SquirixServerExtensionOptions? extensions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        var cluster = SquirixServerConfiguration.ToClusterConfig(options);
        ConfigureBuilder(
            builder,
            cluster,
            options.WaitForRecovery,
            persistenceOptionsOverride: CreatePersistenceOptions(options),
            extensions: extensions);
    }

    public static void ConfigureBuilder(
        WebApplicationBuilder builder,
        ClusterConfig cluster,
        bool waitForRecovery,
        SnapshotTriggerOptions? snapshotOptions = null,
        Func<string, CallPolicy>? callPolicyFactory = null,
        Action<GrpcServiceOptions>? configureGrpc = null,
        Action<IServiceCollection>? servicesConfigure = null,
        PersistenceOptions? persistenceOptionsOverride = null,
        HttpMessageHandler? httpHandlerOverride = null,
        BackpressureOptions? backpressureOptions = null,
        CacheRuntimeOptions? runtimeOptions = null,
        MemoryPressureOptions? memoryPressureOptions = null,
        SecurityOptions? securityOptionsOverride = null,
        TransportExposureOptions? transportExposureOverride = null,
        SquirixServerExtensionOptions? extensions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);

        var uri = new Uri(cluster.Url);
        _ = builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        SquirixKestrelConfiguration.EnsureHttpsTransport(cluster);
        SquirixKestrelConfiguration.ConfigureKestrel(builder, uri, transportExposureOverride);

        _ = builder.Services.AddSquirixValidatedOptions(cluster, snapshotOptions, backpressureOptions, persistenceOptionsOverride, memoryPressureOptions);
        _ = builder.Services.AddSquirixRuntimeServices(runtimeOptions);
        _ = builder.Services.AddSquirixClusterServices(cluster, callPolicyFactory, httpHandlerOverride);
        _ = builder.Services.AddSquirixAdapterEndpointServices();
        _ = builder.Services.AddSquirixPersistenceServices(waitForRecovery);
        _ = builder.Services.AddSquirixCachePipeline(extensions);
        _ = builder.Services.AddSquirixNodeEndpointServices();
        var authEnabled = builder.Services.AddSquirixSecurityServices(securityOptionsOverride);
        SquirixExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(
            uri,
            authEnabled,
            builder.Environment.EnvironmentName,
            transportExposureOverride);
        _ = builder.Services.AddSquirixFrameworkServices(configureGrpc);
        _ = builder.Services.AddSquirixGrpcCorrelationInterceptor();
        servicesConfigure?.Invoke(builder.Services);
        extensions?.ConfigureServices?.Invoke(builder.Services);
        if (extensions is not null)
            _ = builder.Services.AddSingleton(extensions);
        _ = builder.Services.AddSingleton(new SquirixServerEndpointMappingOptions(authEnabled));
    }

    public static WebApplication MapServer(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        _ = app.Use(static async (context, next) =>
        {
            try
            {
                await next();
            }
            catch (ResourceExhaustedException ex)
            {
                await ex.ToHttpResult().ExecuteAsync(context);
            }
            catch (SquirixException ex)
            {
                await ex.ToHttpResult().ExecuteAsync(context);
            }
        });

        var options = app.Services.GetRequiredService<SquirixServerEndpointMappingOptions>();
        if (!options.AuthEnabled)
            return MapEndpoints(app, options.AuthEnabled);
        _ = app.UseAuthentication();
        _ = app.UseAuthorization();

        return MapEndpoints(app, options.AuthEnabled);
    }

    private static PersistenceOptions? CreatePersistenceOptions(SquirixServerOptions options) =>
        string.IsNullOrWhiteSpace(options.DataDirectory)
            ? null
            : new PersistenceOptions { DataDir = options.DataDirectory, StrictFsync = true };

    private static WebApplication MapEndpoints(WebApplication app, bool authEnabled)
    {
        _ = app.MapSquirixEndpoints(authEnabled);
        var extensions = app.Services.GetService<SquirixServerExtensionOptions>();
        extensions?.MapEndpoints?.Invoke(app);
        extensions?.MapEndpointsWithAuthorization?.Invoke(app, authEnabled);
        return app;
    }

    private sealed record SquirixServerEndpointMappingOptions(bool AuthEnabled);
}
