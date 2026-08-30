using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
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
        TopologyOptions cluster,
        ValidatedOptionsArgs args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);

        AddValidatedInstance<TopologyOptions, ConfigValidator>(services, cluster);
        AddValidatedMtlsOptions(services, args);
        AddValidatedInstance<AdmissionOptions, AdmissionOptionsValidator>(services, args.BackpressureOptions ?? new AdmissionOptions());
        var unresolvedMemoryPressure = await PressureBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        var memoryPressure = args.MemoryPressureOptions ?? OptionsResolver.Resolve(unresolvedMemoryPressure, GcMemoryBudgetProvider.Instance);
        AddValidatedInstance<PressureOptions, PressureOptionsValidator>(services, memoryPressure);
        var idempotency = await IdempotencyBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<IdempotencyOptions, IdempotencyOptionsValidator>(services, idempotency);

        if (args.PersistenceOptions != null)
            await AddValidatedPersistenceOptionsAsync(services, args.PersistenceOptions, null, cancellationToken).ConfigureAwait(false);

        var prometheusMetrics = await PrometheusMetricsBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<PrometheusMetricsEndpointOptions, PrometheusEndpointOptionsValidator>(services, prometheusMetrics);
        return services;
    }

    private static void AddValidatedInstance<TOptions, TValidator>(IServiceCollection services, TOptions source)
        where TOptions : class
        where TValidator : class, IValidateOptions<TOptions>
    {
        AddValidatedOptionsInstance(services, source);
        _ = services.AddSingleton<IValidateOptions<TOptions>, TValidator>();
        _ = services.AddHostedService(static sp => new StartupOptionsValidator<TOptions>(
            sp.GetRequiredService<IOptions<TOptions>>(),
            sp.GetRequiredService<IValidateOptions<TOptions>>()));
    }

    private static void AddValidatedMtlsOptions(IServiceCollection services, ValidatedOptionsArgs args)
    {
        var mtlsOptions = args.MtlsOptions ?? MtlsOptionsResolver.ResolveFromEnvironment();
        AddValidatedOptionsInstance(services, mtlsOptions);

        // Factory keeps the internal MtlsOptionsValidator constructor usable with MS.DI.
        _ = services.AddSingleton<IValidateOptions<MtlsOptions>>(static sp => new MtlsOptionsValidator(sp.GetRequiredService<TopologyOptions>()));
        _ = services.AddHostedService(static sp => new StartupOptionsValidator<MtlsOptions>(
            sp.GetRequiredService<IOptions<MtlsOptions>>(),
            sp.GetRequiredService<IValidateOptions<MtlsOptions>>()));

        // Register through the factory overload so the DI container owns and disposes the certificate material on
        // host shutdown. AddSingleton(instance) does not transfer disposal ownership in Microsoft DI, which would
        // leak the loaded X509 certificates.
        _ = args.MtlsMaterial != null ? services.AddSingleton(_ => args.MtlsMaterial) : services.AddSingleton(static provider =>
        {
            var registeredCluster = provider.GetRequiredService<TopologyOptions>();
            var options = provider.GetRequiredService<MtlsOptions>();
            var primaryListenPort = registeredCluster.Uri.IsAbsoluteUri ? registeredCluster.Uri.Port : default(int?);
            return MtlsCertificateMaterial.Load(options, primaryListenPort, MtlsTopology.RequiresInterNodeMtls(registeredCluster));
        });
    }

    private static void AddValidatedOptionsInstance<TOptions>(IServiceCollection services, TOptions source)
        where TOptions : class
    {
        // Register the pre-built instance directly. OptionsFactory would Activator.CreateInstance<TOptions>()
        // (requires a parameterless ctor) and CopyFrom cannot assign init-only properties after construction.
        _ = services.AddSingleton(source);
        _ = services.AddSingleton(Options.Create(source));
        _ = services.AddSingleton<IOptionsMonitor<TOptions>>(new StaticOptionsMonitor<TOptions>(source));
    }

    private static async Task AddValidatedPersistenceOptionsAsync(
        IServiceCollection services,
        PersistenceOptions persistence,
        TriggerOptions? snapshotOptions,
        CancellationToken cancellationToken)
    {
        AddValidatedInstance<PersistenceOptions, PersistenceOptionsValidator>(services, persistence);
        var snapshot = snapshotOptions ?? await SnapshotBootstrap.LoadAsync(cancellationToken).ConfigureAwait(false);
        AddValidatedInstance<TriggerOptions, TriggerOptionsValidator>(services, snapshot);
        var compactionOptions = new JournalCompactionOptions
        {
            Enabled = true,
            MinTailSegments = 2,
            MinTailBytes = 64 * 1024 * 1024,
            MinGap = TimeSpan.FromMinutes(2),
        };
        AddValidatedInstance<JournalCompactionOptions, JournalCompactionOptionsValidator>(services, compactionOptions);
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromSeconds(5) };
        AddValidatedInstance<JournalMetricsExporterOptions, JournalMetricsExporterOptionsValidator>(services, options);
    }

    /// <summary>Loads snapshot trigger settings from <c language="csharp">Squirix.settings.json</c>.</summary>
    private static class SnapshotBootstrap
    {
        /// <summary>Loads snapshot trigger settings using the same settings file discovery as cluster bootstrap.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Loaded snapshot trigger options.</returns>
        internal static async Task<TriggerOptions> LoadAsync(CancellationToken cancellationToken = default)
        {
            var (_, fileMerged) = await UnifiedSettings.TryMergeSnapshotFromFileAsync(new TriggerOptions(), cancellationToken).ConfigureAwait(false);
            return fileMerged;
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container via factory.")]
    [Immutable]
    private sealed class StartupOptionsValidator<TOptions> : IHostedService
        where TOptions : class
    {
        private readonly IOptions<TOptions> _options;
        private readonly IValidateOptions<TOptions> _validator;

        internal StartupOptionsValidator(IOptions<TOptions> options, IValidateOptions<TOptions> validator)
        {
            _options = options;
            _validator = validator;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var result = _validator.Validate(Options.DefaultName, _options.Value);
            return result.Failed ? throw new OptionsValidationException(Options.DefaultName, typeof(TOptions), result.Failures) : Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Immutable]
    private sealed class StaticOptionsMonitor<TOptions> : IOptionsMonitor<TOptions>
        where TOptions : class
    {
        internal StaticOptionsMonitor(TOptions value)
        {
            CurrentValue = value;
        }

        public TOptions CurrentValue { get; }

        public TOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<TOptions, string?> listener) => null;
    }
}
