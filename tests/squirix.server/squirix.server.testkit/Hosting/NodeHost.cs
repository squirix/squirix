using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

internal static class NodeHost
{
    internal static async Task<WebApplication> StartAsync(TopologyOptions cluster, NodeHostStartOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new NodeHostStartOptions();
        var builder = CreateBuilder(options.ConfigureLogging);
        var configureArgs = new CompositionArgsConfigurator(options, cluster.NodeId);

        await ServerHostingComposition.ConfigureBuilderAsync(builder, cluster, configureArgs.Configure, cancellationToken).ConfigureAwait(false);

        var app = builder.Build();
        try
        {
            _ = await ServerHostingComposition.MapServerAsync(app, cancellationToken).ConfigureAwait(false);
            await app.StartAsync(cancellationToken).ConfigureAwait(false);
            return app;
        }
        catch
        {
            await app.DisposeAsync().ConfigureAwait(false);
            throw;
        }
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

    [Immutable]
    private sealed class CompositionArgsConfigurator
    {
        private readonly string _nodeId;
        private readonly NodeHostStartOptions _options;

        internal CompositionArgsConfigurator(NodeHostStartOptions options, string nodeId)
        {
            _options = options;
            _nodeId = nodeId;
        }

        internal void Configure(ICompositionArgs args)
        {
            args.WaitForRecovery = _options.WaitForRecovery;
            args.ConfigureGrpc = _options.ConfigureGrpc;
            args.ServicesConfigure = ComposeServices(_options, _nodeId);
            args.PersistenceOptions = _options.PersistenceOptions;
            args.PeerHandlerFactory = _options.PeerHandlerFactory;
            args.BackpressureOptions = _options.BackpressureOptions;
            args.MemoryPressureOptions = _options.MemoryPressureOptions;
            args.SecurityOptions = _options.SecurityOptions;
            args.MtlsOptions = _options.MtlsOptions;
            args.Certificate = _options.Certificate;
            args.FoundationOnly = _options.FoundationOnly;
        }

        private static Action<IServiceCollection>? ComposeServices(NodeHostStartOptions options, string nodeId)
        {
            if (options.TimeProvider == null && options.ElectionTiming == null)
                return options.ServicesConfigure;

            var timeProvider = options.TimeProvider;
            var election = options.ElectionTiming?.ToOptions(nodeId);
            var userConfigure = options.ServicesConfigure;

            return services =>
            {
                // Register as the base TimeProvider type so the server's PhysicalCache
                // (which resolves TimeProvider via DI) picks up the controllable fake instead of
                // the real-time TimeProvider.System default. RemoveAll guarantees the fake wins
                // over the TryAddSingleton(TimeProvider.System) registered by AddSquirixRuntimeServices.
                if (timeProvider != null)
                    services = services.RemoveAll<TimeProvider>().AddSingleton(timeProvider);

                // Registered before the test hook, so a hook that registers its own election options still wins.
                if (election != null)
                    services = services.RemoveAll<ElectionTimerOptions>().AddSingleton(election);

                userConfigure?.Invoke(services);
            };
        }
    }
}
