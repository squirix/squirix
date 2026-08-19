using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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

namespace Squirix.Server.Node.Hosting;

internal static class PersistenceServiceRegistration
{
    private static readonly string[] ReadyHealthCheckTags = ["ready"];

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "PersistenceRuntime lifetime is transferred to the DI container.")]
    internal static async Task<IServiceCollection> AddPersistenceServicesAsync(
        this IServiceCollection services,
        PersistenceOptions persistence,
        bool waitForRecovery,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistence);

        RegisterPersistenceRuntime(services, await PersistenceRuntime.CreateAsync(persistence, cancellationToken).ConfigureAwait(false));
        RegisterPersistenceHostedServices(services, waitForRecovery);
        return services;
    }

    private static void RegisterPersistenceHostedServices(IServiceCollection services, bool waitForRecovery)
    {
        _ = services.AddSingleton(new RecoveryOptions { BlockOnStart = waitForRecovery });
        _ = services.AddHostedService(static sp => new RecoveryService<object?>(
            sp.GetRequiredService<RecoveryOptions>(),
            sp.GetRequiredService<ILogger<RecoveryService<object?>>>(),
            new RecoveryDependencies<object?>(
                sp.GetRequiredService<PersistenceOptions>(),
                sp.GetRequiredService<Ledger>(),
                sp.GetRequiredService<ILocalCacheRecovery<object?>>(),
                sp.GetRequiredService<JournalStartupGate>(),
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
                sp.GetRequiredService<TopologyOptions>())));
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

    private static void RegisterPersistenceRuntime(IServiceCollection services, PersistenceRuntime runtime)
    {
        _ = services.AddSingleton(runtime);
        _ = services.AddSingleton(runtime.Retention);
        _ = services.AddSingleton<IRetentionCleanupReadinessStatus>(runtime.Retention);
        _ = services.AddSingleton(runtime.Ledger);
        _ = services.AddSingleton(runtime.Gate);
        _ = services.AddSingleton(runtime.JournalCoordinator);
        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalCoordinatorDecorator(
            sp.GetRequiredService<JournalCoordinatorHost>().Coordinator,
            sp.GetRequiredService<IJournalOperationTracer>()));
        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<JournalCoordinatorHost>().Coordinator);
        _ = services.AddSingleton<IExclusiveMaintenanceExecutor>(static sp => sp.GetRequiredService<IJournalCoordinator>());

        // Factories keep internal health-check constructors usable with MS.DI activation.
        _ = services.AddHealthChecks()
                    .Add(
                         new HealthCheckRegistration(
                             "journal_recovery",
                             static sp => new JournalRecoveryReadinessHealthCheck(sp.GetRequiredService<JournalStartupGate>()),
                             HealthStatus.Unhealthy,
                             ReadyHealthCheckTags)).Add(
                         new HealthCheckRegistration(
                             "journal_maintenance",
                             static sp => new JournalMaintenanceReadinessHealthCheck(
                                 sp.GetRequiredService<IJournalCoordinator>(),
                                 sp.GetRequiredService<IJournalCompactionStatus>(),
                                 sp.GetRequiredService<ISnapshotReadinessStatus>()),
                             HealthStatus.Unhealthy,
                             ReadyHealthCheckTags)).Add(
                         new HealthCheckRegistration(
                             "storage_retention_cleanup",
                             static sp => new RetentionCleanupReadinessCheck(sp.GetRequiredService<IRetentionCleanupReadinessStatus>()),
                             HealthStatus.Unhealthy,
                             ReadyHealthCheckTags));
        _ = services.AddSingleton<IJournalOperationTracer, OpenTelemetryJournalOperationTracer>();
        _ = services.AddSingleton<ISnapshotTelemetry, OpenTelemetrySnapshotTelemetry>();

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

    /// <summary>Groups persistence singleton instances for dependency injection registration.</summary>
    private sealed class PersistenceRuntime : IDisposable
    {
        private int _disposed;

        private PersistenceRuntime(PersistenceOptions persistence)
        {
            Retention = new RetentionCleanupReadiness(persistence);
            Ledger = new Ledger(persistence, retentionReadiness: Retention, failureMetrics: ManifestRetentionFailureMetrics.Instance);
            Gate = new JournalStartupGate(false);
            JournalCoordinator = new JournalCoordinatorHost();
        }

        internal JournalStartupGate Gate { get; }

        internal JournalCoordinatorHost JournalCoordinator { get; }

        internal Ledger Ledger { get; }

        internal RetentionCleanupReadiness Retention { get; }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            Ledger.Dispose();
        }

        internal static async Task<PersistenceRuntime> CreateAsync(PersistenceOptions persistence, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(persistence);
            var runtime = new PersistenceRuntime(persistence);
            var manifest = await runtime.Ledger.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            await runtime.JournalCoordinator.InitializeAsync(persistence, manifest, runtime.Ledger, runtime.Gate, cancellationToken).ConfigureAwait(false);
            return runtime;
        }
    }
}
