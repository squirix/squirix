using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

internal static class NodeHost
{
    internal static async Task<WebApplication> StartAsync(TopologyOptions cluster, NodeHostStartOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new NodeHostStartOptions();
        var builder = CreateBuilder(options.ConfigureLogging);
        var configureArgs = new CompositionArgsConfigurer(options);

        await ServerHostingComposition.ConfigureBuilderAsync(
            builder,
            cluster,
            configureArgs.Configure,
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

    private sealed class CompositionArgsConfigurer
    {
        private readonly NodeHostStartOptions _options;

        internal CompositionArgsConfigurer(NodeHostStartOptions options) => _options = options;

        internal void Configure(ICompositionArgs args)
        {
            args.WaitForRecovery = _options.WaitForRecovery;
            args.ConfigureGrpc = _options.ConfigureGrpc;
            args.ServicesConfigure = _options.ServicesConfigure;
            args.PersistenceOptions = _options.PersistenceOptions;
            args.PeerHandlerFactory = _options.PeerHandlerFactory;
            args.BackpressureOptions = _options.BackpressureOptions;
            args.MemoryPressureOptions = _options.MemoryPressureOptions;
            args.SecurityOptions = _options.SecurityOptions;
            args.MtlsOptions = _options.MtlsOptions;
            args.MtlsMaterial = _options.MtlsMaterial;
        }
    }
}
