using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

internal static class NodeHost
{
    internal static async Task<WebApplication> StartAsync(TopologyOptions cluster, NodeHostStartOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new NodeHostStartOptions();
        var diag = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_NODE_START_DIAG"), "1", StringComparison.Ordinal);
        var sw = Stopwatch.StartNew();

        var builder = CreateBuilder(options.ConfigureLogging);
        var createBuilderMs = sw.ElapsedMilliseconds;

        var configureArgs = new CompositionArgsConfigurer(options);

        await ServerHostingComposition.ConfigureBuilderAsync(builder, cluster, configureArgs.Configure, cancellationToken).ConfigureAwait(false);
        var configureMs = sw.ElapsedMilliseconds - createBuilderMs;

        var buildGate = BuildGate.Instance;
        if (buildGate != null)
            await buildGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var app = builder.Build();
            var buildMs = sw.ElapsedMilliseconds - createBuilderMs - configureMs;

            if (diag)
                await Console.Error.WriteLineAsync($"[node-start] node={cluster.NodeId} createBuilder={createBuilderMs}ms configure={configureMs}ms build={buildMs}ms hostStart pending total={sw.ElapsedMilliseconds}ms").ConfigureAwait(false);

            _ = ServerHostingComposition.MapServer(app);

            await app.StartAsync(cancellationToken).ConfigureAwait(false);
            var hostStartMs = sw.ElapsedMilliseconds - createBuilderMs - configureMs - buildMs;

            if (diag)
                await Console.Error.WriteLineAsync($"[node-done] node={cluster.NodeId} hostStart={hostStartMs}ms total={sw.ElapsedMilliseconds}ms").ConfigureAwait(false);

            return app;
        }
        finally
        {
            _ = buildGate?.Release();
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
        // Do NOT set ApplicationName here: pointing it at the Squirix.Server assembly switches the host
        // content root away from the test output directory and makes every builder.Build() pay ~200 ms
        // in logging/configuration initialization (#424).
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });

        _ = builder.Logging.ClearProviders();
        (configureLogging ?? AddDefaultLogging).Invoke(builder.Logging);
        return builder;
    }

    /// <summary>
    /// Caps concurrent node <c>builder.Build()</c> calls: parallel container builds collide on JIT
    /// compilation and each pays several times its solo cost (#424). Default limit is 4; override with
    /// SQUIRIX_BUILD_PARALLELISM (a positive integer), or disable the gate entirely with a non-positive value.
    /// </summary>
    private static class BuildGate
    {
        internal static readonly SemaphoreSlim? Instance = Create();

        private static SemaphoreSlim? Create()
        {
            var raw = Environment.GetEnvironmentVariable("SQUIRIX_BUILD_PARALLELISM");
            if (!string.IsNullOrWhiteSpace(raw))
            {
                return int.TryParse(raw, System.Globalization.CultureInfo.InvariantCulture, out var overrideLimit) && overrideLimit > 0
                    ? new SemaphoreSlim(overrideLimit)
                    : null;
            }

            var defaultLimit = Math.Clamp(Environment.ProcessorCount / 6, 2, 4);
            return new SemaphoreSlim(defaultLimit);
        }
    }

    [Immutable]
    private sealed class CompositionArgsConfigurer
    {
        private readonly NodeHostStartOptions _options;

        internal CompositionArgsConfigurer(NodeHostStartOptions options)
        {
            _options = options;
        }

        internal void Configure(ICompositionArgs args)
        {
            args.WaitForRecovery = _options.WaitForRecovery;
            args.ConfigureGrpc = _options.ConfigureGrpc;
            args.ServicesConfigure = ComposeServices(_options);
            args.PersistenceOptions = _options.PersistenceOptions;
            args.PeerHandlerFactory = _options.PeerHandlerFactory;
            args.BackpressureOptions = _options.BackpressureOptions;
            args.MemoryPressureOptions = _options.MemoryPressureOptions;
            args.SecurityOptions = _options.SecurityOptions;
            args.MtlsOptions = _options.MtlsOptions;
            args.MtlsMaterial = _options.MtlsMaterial;
            args.FoundationOnly = _options.FoundationOnly;
        }

        private static Action<IServiceCollection>? ComposeServices(NodeHostStartOptions options)
        {
            if (options.TimeProvider == null)
                return options.ServicesConfigure;

            var timeProvider = options.TimeProvider;
            var userConfigure = options.ServicesConfigure;

            return services =>
            {
                // Register as the base TimeProvider type so the server's PhysicalCache
                // (which resolves TimeProvider via DI) picks up the controllable fake instead of
                // the real-time TimeProvider.System default. RemoveAll guarantees the fake wins
                // over the TryAddSingleton(TimeProvider.System) registered by AddSquirixRuntimeServices.
                services = services.RemoveAll<TimeProvider>().AddSingleton(timeProvider);
                userConfigure?.Invoke(services);
            };
        }
    }
}
