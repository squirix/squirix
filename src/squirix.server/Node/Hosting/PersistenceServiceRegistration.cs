using System;
using System.Diagnostics.CodeAnalysis;
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

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The PersistenceRuntime singleton is owned by the DI container.")]
    internal static async Task<IServiceCollection> AddPersistenceServicesAsync(
        this IServiceCollection services,
        PersistenceOptions options,
        Meter meter,
        bool waitForRecovery,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = services.AddSingleton(options);

        var failureMetrics = new ManifestRetentionFailureMetrics(meter);
        _ = services.AddSingleton(failureMetrics);

        var runtime = await PersistenceRuntime.CreateAsync(options, failureMetrics, cancellationToken).ConfigureAwait(false);
        _ = services.AddSingleton<PersistenceRuntime>(_ => runtime);

        RegisterPersistenceHostedServices(services, waitForRecovery);
        RegisterPersistenceRuntime(services);

        return services;
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
                sp.GetRequiredService<ISnapshotReader>()),
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
                sp.GetRequiredService<TopologyOptions>()),
            sp.GetRequiredService<CompactionMetrics>()));

        _ = services.AddSingleton<IJournalCompactionStatus>(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());

        _ = services.AddSingleton(static sp => new JournalCompactionController(
            sp.GetRequiredService<PersistenceOptions>(),
            sp.GetRequiredService<Ledger>(),
            sp.GetRequiredService<ISnapshotReader>(),
            sp.GetRequiredService<IJournalCoordinator>(),
            sp.GetRequiredService<ILogger<JournalCompactionController>>()));

        _ = services.AddHostedService<JournalMetricsExporterService>();
    }

    private static void RegisterPersistenceRuntime(IServiceCollection services)
    {
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Retention);
        _ = services.AddSingleton<IRetentionCleanupReadinessStatus>(static sp => sp.GetRequiredService<PersistenceRuntime>().Retention);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Ledger);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().Gate);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<PersistenceRuntime>().JournalCoordinator);

        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalCoordinatorDecorator(
            sp.GetRequiredService<JournalCoordinatorHost>().Coordinator,
            sp.GetRequiredService<IJournalOperationTracer>()));

        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<JournalCoordinatorHost>().Coordinator);
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
        _ = services.AddSingleton(static sp => StoreFactory.CreateReader(sp.GetRequiredService<PersistenceOptions>()));

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
                sp.GetRequiredService<ISnapshotTelemetry>())));
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
                sp.GetRequiredService<ISnapshotReadinessStatus>()),
            HealthStatus.Unhealthy,
            ReadyHealthCheckTags);
        var storageRetentionCleanup = new HealthCheckRegistration(
            "storage_retention_cleanup",
            static sp => new RetentionCleanupReadinessCheck(sp.GetRequiredService<IRetentionCleanupReadinessStatus>()),
            HealthStatus.Unhealthy,
            ReadyHealthCheckTags);
        _ = services.AddHealthChecks().Add(journalRecovery).Add(journalMaintenance).Add(storageRetentionCleanup);
    }

    [Mutable]
    private sealed class PersistenceRuntime : IAsyncDisposable
    {
        private int _disposed;

        private PersistenceRuntime(PersistenceOptions options, ManifestRetentionFailureMetrics failureMetrics)
        {
            Retention = new RetentionCleanupReadiness(options);
            Ledger = new Ledger(options, retentionReadiness: Retention, failureMetrics: failureMetrics);
            Gate = new AsyncManualResetEvent();
            JournalCoordinator = new JournalCoordinatorHost();
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

        internal static async Task<PersistenceRuntime> CreateAsync(PersistenceOptions options, ManifestRetentionFailureMetrics failureMetrics, CancellationToken cancellationToken)
        {
            var runtime = new PersistenceRuntime(options, failureMetrics);
            try
            {
                var manifest = await runtime.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                runtime.JournalCoordinator.Initialize(options, manifest, runtime.Ledger, runtime.Gate);
                return runtime;
            }
            catch
            {
                await runtime.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }
}
