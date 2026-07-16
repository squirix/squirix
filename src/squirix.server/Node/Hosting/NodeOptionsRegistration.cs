using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class NodeOptionsRegistration
{
    internal static async Task<IServiceCollection> AddSquirixValidatedOptionsAsync(
        this IServiceCollection services,
        ClusterConfig cluster,
        ValidatedOptionsArgs args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);

        AddValidatedInstance<ClusterConfig, OptionsValidators.ClusterConfigValidator>(services, cluster);
        var mtlsOptions = args.MtlsOptions ?? MtlsOptionsResolver.ResolveFromEnvironment();
        AddValidatedInstance<MtlsOptions, OptionsValidators.MtlsOptionsValidator>(services, mtlsOptions);
        _ = args.MtlsMaterial is not null ? services.AddSingleton(args.MtlsMaterial) : services.AddSingleton(static provider =>
        {
            var registeredCluster = provider.GetRequiredService<ClusterConfig>();
            var options = provider.GetRequiredService<MtlsOptions>();
            var primaryListenPort = registeredCluster.Uri.IsAbsoluteUri ? registeredCluster.Uri.Port : default(int?);
            return MtlsCertificateMaterial.Load(options, primaryListenPort, MtlsTopology.RequiresInterNodeMtls(registeredCluster));
        });
        AddValidatedInstance<AdmissionOptions, OptionsValidators.BackpressureOptionsValidator>(services, args.BackpressureOptions ?? new AdmissionOptions());
        var unresolvedMemoryPressure = await PressureBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        var memoryPressure = args.MemoryPressureOptions ?? OptionsResolver.Resolve(unresolvedMemoryPressure, GcMemoryBudgetProvider.Instance);
        AddValidatedInstance<PressureOptions, OptionsValidators.MemoryPressureOptionsValidator>(services, memoryPressure);
        var idempotency = await IdempotencyBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<IdempotencyOptions, OptionsValidators.IdempotencyOptionsValidator>(services, idempotency);

        if (args.PersistenceOptions is not null)
        {
            AddValidatedInstance<PersistenceOptions, OptionsValidators.PersistenceOptionsValidator>(services, args.PersistenceOptions);
            var snapshot = args.SnapshotOptions ?? await SnapshotBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
            AddValidatedInstance<TriggerOptions, OptionsValidators.SnapshotTriggerOptionsValidator>(services, snapshot);
            var compactionOptions = new JournalCompactionOptions
            {
                Enabled = true,
                MinTailSegments = 2,
                MinTailBytes = 64 * 1024 * 1024,
                MinGap = TimeSpan.FromMinutes(2),
            };
            AddValidatedInstance<JournalCompactionOptions, OptionsValidators.JournalCompactionOptionsValidator>(services, compactionOptions);
            var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromSeconds(5) };
            AddValidatedInstance<JournalMetricsExporterOptions, OptionsValidators.JournalMetricsExporterOptionsValidator>(services, options);
        }

        var prometheusMetrics = await PrometheusMetricsBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<PrometheusMetricsEndpointOptions, OptionsValidators.PrometheusEndpointOptionsValidator>(services, prometheusMetrics);
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
        _ = services.AddHostedService<OptionsValidators.StartupOptionsValidator<TOptions>>();
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
