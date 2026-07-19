using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.TestKit.Hosting;

internal static class NodeHost
{
    internal static async Task<WebApplication> StartAsync(
        TopologyOptions cluster,
        NodeHostStartOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new NodeHostStartOptions();
        var builder = CreateBuilder(options.ConfigureLogging);
        ExtensionOptions? extensions = null;
        if (options.ConfigureExtensions is not null)
        {
            extensions = new ExtensionOptions();
            options.ConfigureExtensions(extensions);
        }

        await ServerHostingComposition.ConfigureBuilderAsync(
            builder,
            cluster,
            args =>
            {
                args.WaitForRecovery = options.WaitForRecovery;
                args.SnapshotOptions = options.SnapshotOptions;
                args.CallPolicyFactory = options.CallPolicyFactory;
                args.ConfigureGrpc = options.ConfigureGrpc;
                args.ServicesConfigure = options.ServicesConfigure;
                args.PersistenceOptions = options.PersistenceOptions;
                args.PeerHandlerFactory = options.PeerHandlerFactory;
                args.BackpressureOptions = options.BackpressureOptions;
                args.MemoryPressureOptions = options.MemoryPressureOptions;
                args.SecurityOptions = options.SecurityOptions;
                args.Extensions = extensions;
                args.MtlsOptions = options.MtlsOptions;
                args.MtlsMaterial = options.MtlsMaterial;
            },
            cancellationToken).ConfigureAwait(false);

        var app = builder.Build();
        _ = ServerHostingComposition.MapServer(app);

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
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
                ApplicationName = "Squirix.Server",
            });

        _ = builder.Logging.ClearProviders();
        (configureLogging ?? AddDefaultLogging).Invoke(builder.Logging);
        return builder;
    }
}
