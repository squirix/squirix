using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Hosting;

internal static class PersistenceServiceRegistration
{
    private static readonly string[] ReadyHealthCheckTags = ["ready"];

    internal static IServiceCollection AddPersistenceServices(this IServiceCollection services, PersistenceOptions options, Meter meter, bool waitForRecovery)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = services.AddSingleton(options);

        var failureMetrics = new ManifestRetentionFailureMetrics(meter);
        _ = services.AddSingleton(failureMetrics);

        // The runtime is created without I/O so the container owns it from the first resolve; OpenPersistenceAsync opens it after the host is built.
        _ = services.AddSingleton(sp => new PersistenceRuntime(options, failureMetrics, sp.GetRequiredService<ILoggerFactory>(), ResolveClock(sp)));

        RegisterPersistenceHostedServices(services, waitForRecovery);
        RegisterPersistenceRuntime(services);

        return services;
    }

    /// <summary>Opens the manifest and the journal, running journal startup repair, on a built service provider.</summary>
    /// <param name="services">The built service provider that owns the persistence components.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the journal is open.</returns>
    /// <exception cref="InvalidOperationException">Storage is already opened.</exception>
    /// <remarks>
    /// When opening fails the runtime releases what it opened and the failure is rethrown; the container then disposes the components
    /// on application disposal.
    /// </remarks>
    internal static async Task OpenPersistenceAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(services);
        var runtime = services.GetRequiredService<PersistenceRuntime>();
        await runtime.OpenAsync(cancellationToken).ConfigureAwait(false);
        _ = services.GetRequiredService<JournalCoordinatorHost>();
    }

    private static void RegisterPersistenceHostedServices(IServiceCollection services, bool blockOnStart)
    {
        var recoveryOptions = new RecoveryOptions
        {
            BlockOnStart = blockOnStart,
        };
        _ = services.AddSingleton(recoveryOptions);

        _ = services.AddHostedService(static sp => new RecoveryService<object?>(
            sp.GetRequiredService<RecoveryOptions>(),
            sp.GetRequiredService<ILogger<RecoveryService<object?>>>(),
            new RecoveryDependencies<object?>(
                sp.GetRequiredService<PersistenceOptions>(),
                sp.GetRequiredService<Ledger>(),
                sp.GetRequiredService<ILocalCacheRecovery<object?>>(),
                sp.GetRequiredService<AsyncManualResetEvent>(),
                sp.GetRequiredService<RpcMutationIdempotencyStore>(),
                sp.GetRequiredService<ISnapshotReader>(),
                ResolveClock(sp)),
            sp.GetService<IHostApplicationLifetime>()));

        _ = services.AddSingleton<SnapshotTriggerService<object?>>();
        _ = services.AddSingleton<ISnapshotReadinessStatus>(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());

        _ = services.AddSingleton(static sp => new JournalCompactionService<object?>(
            sp.GetRequiredService<ILogger<JournalCompactionService<object?>>>(),
            sp.GetRequiredService<IOptions<JournalCompactionOptions>>(),
            new JournalCompactionDependencies(
                sp.GetRequiredService<Coordinator>(),
                sp.GetRequiredService<IExclusiveMaintenanceExecutor>(),
                sp.GetRequiredService<Ledger>(),
                sp.GetRequiredService<ISnapshotReader>(),
                sp.GetRequiredService<PersistenceOptions>(),
                sp.GetRequiredService<TopologyOptions>(),
                ResolveClock(sp)),
            sp.GetRequiredService<CompactionMetrics>()));

        _ = services.AddSingleton<IJournalCompactionStatus>(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());

        _ = services.AddHostedService<JournalMetricsExporterService>();

        // Stops the journal once every other hosted service has stopped, so their final appends reach it.
        _ = services.AddHostedService(static sp => new JournalStopService(sp.GetRequiredService<JournalCoordinatorHost>().StopAsync));
    }

    private static void RegisterPersistenceRuntime(IServiceCollection services)
    {
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Retention);
        _ = services.AddSingleton<IRetentionCleanupReadinessStatus>(static sp => sp.GetRequiredService<PersistenceRuntime>().Retention);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Ledger);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Gate);

        // The journal publishes manifest rolls to the ledger until its thread is joined, so the ledger must be disposed after the
        // host. The container disposes in reverse order of creation, tracking a service when its factory returns, so resolving the
        // ledger inside this factory tracks it before the host whatever resolves the host first.
        _ = services.AddSingleton(static sp =>
        {
            _ = sp.GetRequiredService<Ledger>();
            return sp.GetRequiredService<PersistenceRuntime>().JournalCoordinator;
        });

        // The journal host is the only owner of the journal lifetime. The container disposes every disposable a factory
        // returns, so no other registration may hand out the raw journal: each goes through the decorator, which does not
        // dispose it. Otherwise, one of them would dispose the journal before the host and abort the container on its failure.
        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalCoordinatorDecorator(
            sp.GetRequiredService<JournalCoordinatorHost>().Coordinator,
            sp.GetRequiredService<IJournalOperationTracer>()));

        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<IJournalCoordinator>());
        _ = services.AddSingleton<IExclusiveMaintenanceExecutor>(static sp => sp.GetRequiredService<IJournalCoordinator>());

        RegisterRuntimeHealthChecks(services);

        _ = services.AddSingleton<IJournalOperationTracer, OpenTelemetryJournalOperationTracer>();
        _ = services.AddSingleton(static sp => new OpenTelemetrySnapshotTelemetry(sp.GetRequiredService<Meter>()));
        _ = services.AddSingleton<ISnapshotTelemetry>(static sp => sp.GetRequiredService<OpenTelemetrySnapshotTelemetry>());

        _ = services.AddSingleton(static sp =>
        {
            var options = sp.GetRequiredService<PersistenceOptions>();
            return StoreFactory.CreateWriter(options);
        });
        _ = services.AddSingleton(static _ => StoreFactory.CreateReader());

        _ = services.AddSingleton(static sp => new Coordinator(
            sp.GetRequiredService<TriggerOptions>(),
            sp.GetRequiredService<IJournalMetrics>(),
            new CoordinatorDependencies(
                sp.GetRequiredService<ISnapshotEntryCapture>(),
                sp.GetRequiredService<ISnapshotWriter>(),
                sp.GetRequiredService<Ledger>(),
                sp.GetRequiredService<IIdempotencySnapshotExporter>(),
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<IBackgroundSnapshotMemoryThrottle>(),
                sp.GetRequiredService<ISnapshotTelemetry>()),
            ResolveClock(sp)));
    }

    private static void RegisterRuntimeHealthChecks(IServiceCollection services)
    {
        var journalRecovery = new HealthCheckRegistration(
            "journal_recovery",
            static sp => new JournalRecoveryReadinessHealthCheck(sp.GetRequiredService<AsyncManualResetEvent>()),
            HealthStatus.Unhealthy,
            ReadyHealthCheckTags);

        var journalMaintenance = new HealthCheckRegistration(
            "journal_maintenance",
            static sp => new JournalMaintenanceReadinessHealthCheck(
                sp.GetRequiredService<IJournalCoordinator>(),
                sp.GetRequiredService<IJournalCompactionStatus>(),
                sp.GetRequiredService<ISnapshotReadinessStatus>(),
                FindStallProbe(sp.GetRequiredService<JournalCoordinatorHost>().Coordinator),
                sp.GetRequiredService<PersistenceOptions>().JournalStallDegradedThreshold,
                ResolveClock(sp)),
            HealthStatus.Unhealthy,
            ReadyHealthCheckTags);
        var storageRetentionCleanup = new HealthCheckRegistration(
            "storage_retention_cleanup",
            static sp => new RetentionCleanupReadinessCheck(sp.GetRequiredService<IRetentionCleanupReadinessStatus>()),
            HealthStatus.Unhealthy,
            ReadyHealthCheckTags);
        _ = services.AddHealthChecks().Add(journalRecovery).Add(journalMaintenance).Add(storageRetentionCleanup);
    }

    /// <summary>Resolves the server clock; the system clock when the container has none registered.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The clock the persistence components measure time with.</returns>
    private static TimeProvider ResolveClock(IServiceProvider sp) => sp.GetService<TimeProvider>() ?? TimeProvider.System;

    /// <summary>Finds the segment I/O stall probe of the owned journal; a coordinator without one reports no stall.</summary>
    /// <param name="coordinator">The journal coordinator owned by the host.</param>
    /// <returns>The probe, or <see langword="null" /> when the coordinator does not record its segment I/O.</returns>
    private static JournalStallProbe? FindStallProbe(IJournalCoordinator coordinator) => coordinator is IJournalStallProbeSource state ? state.StallProbe : null;

    [Mutable]
    private sealed class PersistenceRuntime : IAsyncDisposable
    {
        private readonly PersistenceOptions _options;
        private int _disposed;
        private int _opened;

        internal PersistenceRuntime(PersistenceOptions options, ManifestRetentionFailureMetrics failureMetrics, ILoggerFactory loggerFactory, TimeProvider timeProvider)
        {
            ArgumentNullException.ThrowIfNull(loggerFactory);
            _options = options;
            Retention = new RetentionCleanupReadiness(options, timeProvider);
            Ledger = new Ledger(options, loggerFactory.CreateLogger<Ledger>(), Retention, failureMetrics);
            Gate = new AsyncManualResetEvent();
            JournalCoordinator = new JournalCoordinatorHost(loggerFactory, timeProvider);
        }

        internal AsyncManualResetEvent Gate { get; }

        internal JournalCoordinatorHost JournalCoordinator { get; }

        internal Ledger Ledger { get; }

        internal RetentionCleanupReadiness Retention { get; }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            await JournalCoordinator.DisposeAsync().ConfigureAwait(false);
            Ledger.Dispose();
        }

        /// <summary>Reads the manifest and opens the journal; on failure releases what was opened and rethrows.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when the journal is open.</returns>
        /// <exception cref="InvalidOperationException">The runtime is already opened.</exception>
        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _opened, 1) != 0)
                throw new InvalidOperationException("Squirix storage is already opened.");

            try
            {
                var manifest = await Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                JournalCoordinator.Open(_options, manifest, Ledger, Gate);
            }
            catch
            {
                await DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }
}
