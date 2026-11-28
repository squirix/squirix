using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class NodeOptionsRegistration
{
    internal static async Task<NodeServices> AddSquirixValidatedOptionsAsync(
        this NodeServices services,
        TopologyOptions cluster,
        ValidatedOptionsArgs args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);

        AddValidatedInstance<TopologyOptions, ConfigValidator>(services, cluster);
        AddValidatedMtlsOptions(services, args);
        AddValidatedInstance<AdmissionOptions, AdmissionOptionsValidator>(services, args.BackpressureOptions ?? new AdmissionOptions());
        AddValidatedInstance<PressureOptions, PressureOptionsValidator>(services, args.MemoryPressureOptions);

        if (args.PersistenceOptions != null)
            await AddValidatedPersistenceOptionsAsync(services, args.PersistenceOptions, args.SettingsFilePath, cancellationToken).ConfigureAwait(false);

        var prometheusMetrics = args.PrometheusMetricsOptions ?? await PrometheusMetricsBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<PrometheusMetricsEndpointOptions, PrometheusEndpointOptionsValidator>(services, prometheusMetrics);
        return services;
    }

    private static void AddValidatedInstance<TOptions, TValidator>(NodeServices services, TOptions source)
        where TOptions : class
        where TValidator : class, ISettingsRule<TOptions>, new()
    {
        _ = services.Use(source);
        _ = services.Own<ISettingsRule<TOptions>>(static _ => new TValidator());
        _ = services.RunService(static sp => new StartupOptionsValidator<TOptions>(
            sp.Require<TOptions>(),
            sp.Require<ISettingsRule<TOptions>>()));
    }

    private static void AddValidatedMtlsOptions(NodeServices services, ValidatedOptionsArgs args)
    {
        _ = services.Use(args.MtlsOptions);

        _ = services.Own<ISettingsRule<MtlsOptions>>(static sp => new MtlsOptionsValidator(sp.Require<TopologyOptions>()));
        _ = services.RunService(static sp => new StartupOptionsValidator<MtlsOptions>(
            sp.Require<MtlsOptions>(),
            sp.Require<ISettingsRule<MtlsOptions>>()));

        // The node owns the loader's certificate hold. Outbound clients and listeners retain separate
        // holds until their connections drain; the last holder releases the certificate material.
        _ = services.Own(_ => args.Certificate);
    }

    private static async Task AddValidatedPersistenceOptionsAsync(
        NodeServices services,
        PersistenceOptions persistence,
        string? settingsFilePath,
        CancellationToken cancellationToken)
    {
        AddValidatedInstance<PersistenceOptions, PersistenceOptionsValidator>(services, persistence);
        var snapshot = await UnifiedSettings.LoadSnapshotOptionsAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<TriggerOptions, TriggerOptionsValidator>(services, snapshot);
    }

    [Immutable]
    private sealed class StartupOptionsValidator<TOptions> : INodeService
        where TOptions : class
    {
        private readonly TOptions _options;
        private readonly ISettingsRule<TOptions> _validator;

        internal StartupOptionsValidator(TOptions options, ISettingsRule<TOptions> validator)
        {
            _options = options;
            _validator = validator;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var failures = _validator.Check(_options);
            return failures.Count == 0 ? Task.CompletedTask : throw new InvalidOperationException(string.Join("; ", failures));
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
