using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Hosting;

internal static class PersistenceServiceRegistration
{
    internal static NodeServices AddPersistenceServices(this NodeServices services, PersistenceOptions options, bool waitForRecovery, bool metricsEnabled = false)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = services.Use(options);

        // The runtime is created without I/O so the container owns it from the first resolve; OpenPersistenceAsync opens it after the host is built.
        _ = services.Own(sp => new PersistenceRuntime(
            options,
            new ManifestRetentionFailureMetrics(sp.Find<Meter>()),
            sp.Require<IServerLoggerFactory>(),
            ResolveClock(sp)));

        RegisterPersistenceHostedServices(services, waitForRecovery, metricsEnabled);
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
        var runtime = services.Require<PersistenceRuntime>();
        await runtime.OpenAsync(cancellationToken).ConfigureAwait(false);
        _ = services.Require<JournalCoordinatorHost>();
    }

    private static void RegisterPersistenceHostedServices(NodeServices services, bool blockOnStart, bool metricsEnabled)
    {
        var recoveryOptions = new RecoveryOptions
        {
            BlockOnStart = blockOnStart,
        };
        _ = services.Use(recoveryOptions);

        _ = services.RunService(static sp => new RecoveryService<object?>(
            sp.Require<RecoveryOptions>(),
            sp.Require<IServerLoggerFactory>().CreateLogger<RecoveryService<object?>>(),
            new RecoveryDependencies<object?>(
                sp.Require<PersistenceOptions>(),
                sp.Require<Ledger>(),
                sp.Require<ILocalCacheRecovery<object?>>(),
                sp.Require<AsyncManualResetEvent>(),
                sp.Require<ISnapshotReader>(),
                ResolveClock(sp))
            {
                Expiry = sp.Require<CacheExpiryAuthority>(),
            },
            sp.Find<INodeLifetime>()));

        _ = services.Own(static sp =>
        {
            var service = new SnapshotTriggerService(
                sp.Require<IServerLoggerFactory>().CreateLogger<SnapshotTriggerService>(),
                sp.Require<Coordinator>(),
                sp.Require<IJournalCoordinator>(),
                sp.Require<TimeProvider>());
            sp.Require<OpenTelemetrySnapshotTelemetry>().ObserveConsecutiveFailures(sp.Require<TopologyOptions>().NodeId, () => service.CurrentFailure?.ConsecutiveFailures ?? 0);
            return service;
        });
        _ = services.Own<ISnapshotReadinessStatus>(static sp => sp.Require<SnapshotTriggerService>());
        _ = services.RunService(static sp => sp.Require<SnapshotTriggerService>());

        if (metricsEnabled)
        {
            _ = services.RunService(static sp => new JournalMetricsExporterService(
                sp.Require<PersistenceOptions>(),
                sp.Require<TopologyOptions>(),
                sp.Require<IServerLoggerFactory>().CreateLogger<JournalMetricsExporterService>(),
                sp.Require<Meter>()));
        }

        // Stops the journal once every other hosted service has stopped, so their final appends reach it.
        _ = services.RunService(static sp => new JournalStopService(sp.Require<JournalCoordinatorHost>().StopAsync));
    }

    private static void RegisterPersistenceRuntime(NodeServices services)
    {
        _ = services.Own(static sp => sp.Require<PersistenceRuntime>().Retention);
        _ = services.Own<IRetentionCleanupReadinessStatus>(static sp => sp.Require<PersistenceRuntime>().Retention);
        _ = services.Own(static sp => sp.Require<PersistenceRuntime>().Ledger);
        _ = services.Own(static sp => sp.Require<PersistenceRuntime>().Gate);

        // The journal publishes manifest rolls to the ledger until its thread is joined, so the ledger must be disposed after the
        // host. The container disposes in reverse order of creation, tracking a service when its factory returns, so resolving the
        // ledger inside this factory tracks it before the host whatever resolves the host first.
        _ = services.Own(static sp =>
        {
            _ = sp.Require<Ledger>();
            return sp.Require<PersistenceRuntime>().JournalCoordinator;
        });

        // The journal host is the only owner of the journal lifetime. The container disposes every disposable a factory
        // returns, so no other registration may hand out the raw journal: each goes through the decorator, which does not
        // dispose it. Otherwise, one of them would dispose the journal before the host and abort the container on its failure.
        _ = services.Own<IJournalCoordinator>(static sp => new TracingJournalCoordinatorDecorator(
            sp.Require<JournalCoordinatorHost>().Coordinator,
            sp.Require<IJournalOperationTracer>()));

        _ = services.Own<IJournalMetrics>(static sp => sp.Require<IJournalCoordinator>());

        RegisterRuntimeHealthChecks(services);

        _ = services.Own<IJournalOperationTracer>(static _ => new OpenTelemetryJournalOperationTracer());
        _ = services.Own(static sp => new OpenTelemetrySnapshotTelemetry(sp.Find<Meter>()));
        _ = services.Own<ISnapshotTelemetry>(static sp => sp.Require<OpenTelemetrySnapshotTelemetry>());

        _ = services.Own(static sp =>
        {
            var options = sp.Require<PersistenceOptions>();
            return StoreFactory.CreateWriter(options);
        });
        _ = services.Own(static _ => StoreFactory.CreateReader());

        _ = services.Own(static sp => new Coordinator(
            sp.Require<TriggerOptions>(),
            sp.Require<IJournalMetrics>(),
            new CoordinatorDependencies(
                sp.Require<ISnapshotEntryCapture>(),
                sp.Require<ISnapshotWriter>(),
                sp.Require<Ledger>(),
                sp.Require<TopologyOptions>().NodeId,
                sp.Require<ISnapshotTelemetry>()),
            ResolveClock(sp)));
    }

    private static void RegisterRuntimeHealthChecks(NodeServices services)
    {
        var journalRecovery = new ReadinessCheck(
            "journal_recovery",
            static sp => new JournalRecoveryReadinessHealthCheck(sp.Require<AsyncManualResetEvent>()));

        var journalMaintenance = new ReadinessCheck(
            "journal_maintenance",
            static sp => new JournalMaintenanceReadinessHealthCheck(
                sp.Require<IJournalCoordinator>(),
                sp.Require<ISnapshotReadinessStatus>(),
                FindStallProbe(sp.Require<JournalCoordinatorHost>().Coordinator),
                sp.Require<PersistenceOptions>().JournalStallDegradedThreshold,
                ResolveClock(sp)));
        var storageRetentionCleanup = new ReadinessCheck(
            "storage_retention_cleanup",
            static sp => new RetentionCleanupReadinessCheck(sp.Require<IRetentionCleanupReadinessStatus>()));
        _ = services.Use(journalRecovery).Use(journalMaintenance).Use(storageRetentionCleanup);
    }

    /// <summary>Resolves the server clock.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The clock the persistence components measure time with.</returns>
    private static TimeProvider ResolveClock(IServiceProvider sp) => sp.Require<TimeProvider>();

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

        internal PersistenceRuntime(PersistenceOptions options, ManifestRetentionFailureMetrics failureMetrics, IServerLoggerFactory loggerFactory, TimeProvider timeProvider)
        {
            ArgumentNullException.ThrowIfNull(loggerFactory);
            _options = options;
            Retention = new RetentionCleanupReadiness(options, timeProvider);
            Ledger = new Ledger(options, loggerFactory.CreateLogger<Ledger>(), failureMetrics, Retention);
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
