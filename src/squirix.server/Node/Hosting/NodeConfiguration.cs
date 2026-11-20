using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class NodeConfiguration
{
    internal static async Task ConfigureNodeAsync(
        IHostApplicationBuilder builder,
        SquirixServerOptions options,
        Action<CompositionArgs>? configureComposition,
        CancellationToken cancellationToken)
    {
        var persistenceOptions = ResolvePersistenceOptions(options);
        await ServerHostingComposition.ConfigureBuilderAsync(
            builder,
            Configurator.ToClusterConfig(options),
            args =>
            {
                args.WaitForRecovery = options.WaitForRecovery;
                args.PersistenceOptions = persistenceOptions;
                args.BackpressureOptions = options.Backpressure.ToAdmissionOptions();
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
