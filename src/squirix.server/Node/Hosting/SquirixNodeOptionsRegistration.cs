using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixNodeOptionsRegistration
{
    public static async Task<IServiceCollection> AddSquirixValidatedOptionsAsync(
        this IServiceCollection services,
        ClusterConfig cluster,
        SnapshotTriggerOptions? snapshotOptions,
        BackpressureOptions? backpressureOptions,
        PersistenceOptions? persistence,
        MemoryPressureOptions? memoryPressureOptionsOverride,
        MtlsOptions? mtlsOptionsOverride = null,
        MtlsCertificateMaterial? mtlsMaterialOverride = null,
        CancellationToken cancellationToken = default)
    {
        AddValidatedInstance<ClusterConfig, SquirixOptionsValidators.ClusterConfigValidator>(services, cluster);
        var mtlsOptions = mtlsOptionsOverride ?? MtlsOptionsResolver.ResolveFromEnvironment();
        AddValidatedInstance<MtlsOptions, SquirixOptionsValidators.MtlsOptionsValidator>(services, mtlsOptions);
        _ = mtlsMaterialOverride is not null ? services.AddSingleton(mtlsMaterialOverride) : services.AddSingleton(static provider =>
        {
            var registeredCluster = provider.GetRequiredService<ClusterConfig>();
            var options = provider.GetRequiredService<MtlsOptions>();
            var primaryListenPort = Uri.TryCreate(registeredCluster.Url, UriKind.Absolute, out var listenUri) ? listenUri.Port : default(int?);
            return MtlsCertificateMaterial.Load(options, primaryListenPort, MtlsTopology.RequiresInterNodeMtls(registeredCluster));
        });
        AddValidatedInstance<BackpressureOptions, SquirixOptionsValidators.BackpressureOptionsValidator>(services, backpressureOptions ?? new BackpressureOptions());
        var unresolvedMemoryPressure = await MemoryPressureBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        var memoryPressure = memoryPressureOptionsOverride ?? MemoryPressureOptionsResolver.Resolve(unresolvedMemoryPressure, GcMemoryBudgetProvider.Instance);
        AddValidatedInstance<MemoryPressureOptions, SquirixOptionsValidators.MemoryPressureOptionsValidator>(services, memoryPressure);

        if (persistence is not null)
        {
            AddValidatedInstance<PersistenceOptions, SquirixOptionsValidators.PersistenceOptionsValidator>(services, persistence);
            var snapshot = snapshotOptions ?? new SnapshotTriggerOptions
            {
                Enabled = true,
                SnapshotInterval = TimeSpan.FromMinutes(5),
                SnapshotEveryNOps = 250_000,
                SnapshotEveryNBytes = 128 * 1024 * 1024,
                MinGapBetweenSnapshots = TimeSpan.FromMinutes(1),
            };
            AddValidatedInstance<SnapshotTriggerOptions, SquirixOptionsValidators.SnapshotTriggerOptionsValidator>(services, snapshot);
            var compactionOptions = new JournalCompactionOptions
            {
                Enabled = true,
                MinTailSegments = 2,
                MinTailBytes = 64 * 1024 * 1024,
                MinGap = TimeSpan.FromMinutes(2),
            };
            AddValidatedInstance<JournalCompactionOptions, SquirixOptionsValidators.JournalCompactionOptionsValidator>(services, compactionOptions);
            var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromSeconds(5) };
            AddValidatedInstance<JournalMetricsExporterOptions, SquirixOptionsValidators.JournalMetricsExporterOptionsValidator>(services, options);
        }

        var prometheusMetrics = await PrometheusMetricsBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<PrometheusMetricsEndpointOptions, SquirixOptionsValidators.PrometheusMetricsEndpointOptionsValidator>(services, prometheusMetrics);
        return services;
    }

    private static void AddValidatedInstance<TOptions, TValidator>(IServiceCollection services, TOptions source)
        where TOptions : class
        where TValidator : class, IValidateOptions<TOptions>
    {
        // Register the pre-built instance directly. OptionsFactory would Activator.CreateInstance<TOptions>()
        // (requires a parameterless ctor) and CopyFrom cannot assign init-only properties after construction.
        _ = services.AddSingleton(source);
        _ = services.AddSingleton(Options.Create(source));
        _ = services.AddSingleton<IOptionsMonitor<TOptions>>(new StaticOptionsMonitor<TOptions>(source));
        _ = services.AddSingleton<IValidateOptions<TOptions>, TValidator>();
        _ = services.AddHostedService<SquirixOptionsValidators.StartupOptionsValidator<TOptions>>();
    }

    private sealed class StaticOptionsMonitor<TOptions> : IOptionsMonitor<TOptions>
        where TOptions : class
    {
        public StaticOptionsMonitor(TOptions value)
        {
            CurrentValue = value;
        }

        public TOptions CurrentValue { get; }

        public TOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<TOptions, string?> listener) => null;
    }
}
