using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixNodeOptionsRegistration
{
    public static IServiceCollection AddSquirixValidatedOptions(
        this IServiceCollection services,
        ClusterConfig cluster,
        SnapshotTriggerOptions? snapshotOptions,
        BackpressureOptions? backpressureOptions,
        PersistenceOptions? persistenceOptionsOverride,
        MemoryPressureOptions? memoryPressureOptionsOverride,
        MtlsOptions? mtlsOptionsOverride = null,
        MtlsCertificateMaterial? mtlsMaterialOverride = null)
    {
        AddValidatedInstance<ClusterConfig, SquirixOptionsValidators.ClusterConfigValidator>(services, cluster);
        var mtlsOptions = mtlsOptionsOverride ?? MtlsOptionsResolver.ResolveFromEnvironment();
        AddValidatedInstance<MtlsOptions, SquirixOptionsValidators.MtlsOptionsValidator>(services, mtlsOptions);
        _ = mtlsMaterialOverride is not null
            ? services.AddSingleton(mtlsMaterialOverride)
            : services.AddSingleton(static provider =>
            {
                var registeredCluster = provider.GetRequiredService<ClusterConfig>();
                var options = provider.GetRequiredService<MtlsOptions>();
                var primaryListenPort = Uri.TryCreate(registeredCluster.Url, UriKind.Absolute, out var listenUri) ? listenUri.Port : (int?)null;
                return MtlsCertificateMaterial.Load(
                    options,
                    primaryListenPort,
                    MtlsTopology.RequiresInterNodeMtls(registeredCluster));
            });
        AddValidatedInstance<BackpressureOptions, SquirixOptionsValidators.BackpressureOptionsValidator>(services, backpressureOptions ?? new BackpressureOptions());
        var memoryPressure = memoryPressureOptionsOverride ?? MemoryPressureOptionsResolver.Resolve(MemoryPressureBootstrap.Load(), GcMemoryBudgetProvider.Instance);
        AddValidatedInstance<MemoryPressureOptions, SquirixOptionsValidators.MemoryPressureOptionsValidator>(services, memoryPressure);

        if (persistenceOptionsOverride is not null)
        {
            var dataDir = string.IsNullOrWhiteSpace(persistenceOptionsOverride.DataDir)
                ? GetDefaultDataDir(cluster.ClusterId, cluster.NodeId)
                : persistenceOptionsOverride.DataDir;
            var persistence = persistenceOptionsOverride with { DataDir = dataDir };

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

        AddValidatedInstance<PrometheusMetricsEndpointOptions, SquirixOptionsValidators.PrometheusMetricsEndpointOptionsValidator>(services, PrometheusMetricsBootstrap.Load());
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

    private static string GetDefaultDataDir(string clusterId, string nodeId)
    {
        var testRoot = EnvVariables.ReadString("SQUIRIX_TEST_ROOT");
        if (!string.IsNullOrWhiteSpace(testRoot))
            return PathEx.Combine(testRoot, clusterId, nodeId);

        var dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

        if (string.IsNullOrWhiteSpace(dir) && !OperatingSystem.IsWindows())
        {
            dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);
        }

        return string.IsNullOrWhiteSpace(dir)
            ? throw new InvalidOperationException(
                "Cannot determine default data directory: LocalApplicationData is not available. Set PersistenceOptions.DataDir explicitly or define the HOME / XDG_DATA_HOME environment variable.")
            : PathEx.Combine(dir, "squirix", clusterId, nodeId);
    }

    private sealed class StaticOptionsMonitor<TOptions> : IOptionsMonitor<TOptions>
        where TOptions : class
    {
        public StaticOptionsMonitor(TOptions value) => CurrentValue = value;

        public TOptions CurrentValue { get; }

        public TOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<TOptions, string?> listener) => null;
    }
}
