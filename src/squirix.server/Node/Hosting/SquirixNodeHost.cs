using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixNodeHost
{
    public static async Task<WebApplication> StartAsync(
        ClusterConfig cluster,
        Action<ILoggingBuilder>? configureLogging = null,
        bool waitForRecovery = true,
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
        Action<SquirixServerExtensionOptions>? configureExtensions = null,
        MtlsOptions? mtlsOptionsOverride = null,
        MtlsCertificateMaterial? mtlsMaterialOverride = null,
        CancellationToken cancellationToken = default)
    {
        var builder = CreateBuilder(configureLogging);
        SquirixServerExtensionOptions? extensions = null;
        if (configureExtensions is not null)
        {
            extensions = new SquirixServerExtensionOptions();
            configureExtensions(extensions);
        }

        SquirixServerHostingComposition.ConfigureBuilder(
            builder,
            cluster,
            waitForRecovery,
            snapshotOptions,
            callPolicyFactory,
            configureGrpc,
            servicesConfigure,
            persistenceOptionsOverride,
            httpHandlerOverride,
            backpressureOptions,
            runtimeOptions,
            memoryPressureOptions,
            securityOptionsOverride,
            extensions,
            mtlsOptionsOverride: mtlsOptionsOverride,
            mtlsMaterialOverride: mtlsMaterialOverride);

        var app = builder.Build();
        _ = SquirixServerHostingComposition.MapServer(app);

        await app.StartAsync(cancellationToken);
        return app;
    }

    private static void AddDefaultLogging(ILoggingBuilder b)
    {
        _ = b.AddConsole();
        _ = b.AddDebug();
        _ = b.AddFilter("Grpc", LogLevel.Information);
        _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Information);
        _ = b.AddFilter("Squirix", LogLevel.Debug);
    }

    private static WebApplicationBuilder CreateBuilder(Action<ILoggingBuilder>? configureLogging)
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                Args = [],
                ApplicationName = typeof(SquirixNodeHost).Assembly.GetName().Name,
            });

        _ = builder.Logging.ClearProviders();
        (configureLogging ?? AddDefaultLogging).Invoke(builder.Logging);
        return builder;
    }
}
