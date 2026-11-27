using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server;

internal static class NodeConfiguration
{
    internal static async Task ConfigureNodeAsync(
        NodeServices services,
        SquirixServerOptions options,
        Action<CompositionArgs>? configureComposition,
        CancellationToken cancellationToken)
    {
        var persistenceOptions = ResolvePersistenceOptions(options);
        await ServerHostingComposition.ConfigureBuilderAsync(
            services,
            Configurator.ToClusterConfig(options),
            args =>
            {
                args.Eviction = options.Eviction;
                args.SettingsFilePath = options.SettingsFilePath;
                args.WaitForRecovery = options.WaitForRecovery;
                args.PersistenceOptions = persistenceOptions;
                args.BackpressureOptions = options.Backpressure.ToAdmissionOptions();
                args.MemoryAccountingEnabled = options.MemoryAccountingEnabled;
                args.MetricsEnabled = options.MetricsEnabled;
                args.PrometheusMetricsOptions = new PrometheusMetricsEndpointOptions
                {
                    Enabled = options.PrometheusMetricsEnabled,
                    Path = options.PrometheusMetricsPath,
                };
                configureComposition?.Invoke(args);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static PersistenceOptions? ResolvePersistenceOptions(SquirixServerOptions options)
    {
        if (!options.PersistenceEnabled)
            return null;

        var opt = options.Journal.ToPersistenceOptions();

        return string.IsNullOrWhiteSpace(options.DataDirectory) ? opt : opt with { DataDir = FilePathValidator.ResolveValidatedDirectoryPath(options.DataDirectory) };
    }
}
