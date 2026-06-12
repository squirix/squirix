using System;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
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
            persistenceOptionsOverride: ResolvePersistenceOptions(options),
            extensions: extensions);
    }

    [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Cluster mTLS material is registered as a singleton and disposed by the host on shutdown.")]
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
        SquirixServerExtensionOptions? extensions = null,
        MtlsOptions? mtlsOptionsOverride = null,
        MtlsCertificateMaterial? mtlsMaterialOverride = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);

        var persistenceEnabled = persistenceOptionsOverride is not null;
        var uri = new Uri(cluster.Url);
        _ = builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        SquirixKestrelConfiguration.EnsureHttpsTransport(cluster);
        var requiresInterNodeMtls = MtlsTopology.RequiresInterNodeMtls(cluster);
        var mtlsOptions = mtlsOptionsOverride ?? MtlsOptionsResolver.ResolveFromEnvironment();
        var mtlsMaterial = mtlsMaterialOverride
            ?? MtlsCertificateMaterial.Load(mtlsOptions, uri.Port, requiresInterNodeMtls);
        SquirixKestrelConfiguration.ConfigureKestrel(builder, uri, mtlsOptions, mtlsMaterial);

        _ = builder.Services.AddSquirixValidatedOptions(
            cluster,
            snapshotOptions,
            backpressureOptions,
            persistenceOptionsOverride,
            memoryPressureOptions,
            mtlsOptionsOverride: mtlsOptions,
            mtlsMaterialOverride: mtlsMaterial);
        _ = builder.Services.AddSquirixRuntimeServices(runtimeOptions);
        _ = builder.Services.AddSquirixClusterServices(cluster, callPolicyFactory, httpHandlerOverride);
        if (persistenceEnabled)
            _ = builder.Services.AddSquirixPersistenceServices(waitForRecovery);

        _ = builder.Services.AddSquirixCachePipeline(extensions, persistenceEnabled);
        _ = builder.Services.AddSquirixNodeEndpointServices(persistenceEnabled);
        var authEnabled = builder.Services.AddSquirixSecurityServices(securityOptionsOverride);
        SquirixExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(uri, authEnabled);
        _ = builder.Services.AddSquirixFrameworkServices(builder.Environment.IsDevelopment(), configureGrpc);
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

    private static PersistenceOptions? ResolvePersistenceOptions(SquirixServerOptions options)
    {
        if (!options.PersistenceEnabled)
            return null;

        return string.IsNullOrWhiteSpace(options.DataDirectory)
            ? new PersistenceOptions
            {
                JournalMaxSegmentMb = 64,
                FlushIntervalMs = 10,
                SnapshotIntervalSec = 60,
            }
            : new PersistenceOptions { DataDir = options.DataDirectory };
    }

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
