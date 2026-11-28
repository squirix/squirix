using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

internal static class NodeHost
{
    internal static async Task<ServerApplication> StartAsync(TopologyOptions cluster, NodeHostStartOptions options, CancellationToken cancellationToken)
    {
        var services = TestNodeBuilder.CreateServices(options.ConsoleLogging);

        var configureArgs = new CompositionArgsConfigurator(options, cluster.NodeId);
        await TestNodeServices.ConfigureBuilderAsync(services, cluster, configureArgs.Configure, cancellationToken).ConfigureAwait(false);

        configureArgs.ConfigureServices(services);

        if (options.PeerOptionsFactory is { } createPeerOptions)
            _ = services.AddSingleton(createPeerOptions);

        if (options.TrackRequests)
            InFlightRequestTracker.AddTo(services);

        var app = services.BuildNode();
        await SquirixServer.ApplicationHandle.StartWithRollbackAsync(app, static (node, token) => ServerHostingComposition.OpenStorageAsync(node.Services, token), cancellationToken).ConfigureAwait(false);
        return app;
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

        internal void Configure(CompositionArgs args)
        {
            args.RespUri = _options.Resp?.ToListenUri();
            args.ManagementUri = _options.ManagementUri ?? new Uri("https://127.0.0.1:0");
            args.ServerCertificate = _options.ServerCertificate ?? DevelopmentCertificate.Shared;
            args.WaitForRecovery = _options.WaitForRecovery;
            args.PersistenceOptions = _options.PersistenceOptions;
            args.MetricsEnabled = _options.MetricsEnabled;
            args.PrometheusMetricsOptions = new PrometheusMetricsEndpointOptions { Enabled = _options.PrometheusMetricsEnabled };
            args.BackpressureOptions = _options.BackpressureOptions;
            args.MemoryPressureOptions = _options.MemoryPressureOptions;
            if (_options.EvictionBudgetBytes is { } budget)
            {
                args.Eviction = new Squirix.Server.Core.EvictionOptions { EvictOnMemoryPressure = true };
                args.MemoryPressureOptions = new Squirix.Server.Node.MemoryPressure.PressureOptions { MaxEstimatedCacheBytes = budget, MemoryAccountingEnabled = true };
            }

            args.SecurityOptions = _options.SecurityOptions;
            args.MtlsOptions = _options.MtlsOptions;
            args.Certificate = _options.Certificate;
            args.FoundationOnly = _options.FoundationOnly;
        }

        internal void ConfigureServices(TestServiceCollection services) => ComposeServices(_options, _nodeId)?.Invoke(services);

        private static Action<TestServiceCollection>? ComposeServices(NodeHostStartOptions options, string nodeId)
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

                // Registered before the test hook: a hook that adds its own election options wins, one that only tries to add them loses.
                // The last registration is the one resolved.
                if (election != null)
                    services = services.RemoveAll<ElectionTimerOptions>().AddSingleton(election);

                userConfigure?.Invoke(services);
            };
        }
    }
}
