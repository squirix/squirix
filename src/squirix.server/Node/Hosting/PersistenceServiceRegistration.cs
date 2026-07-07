using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class PersistenceServiceRegistration
{
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "PersistenceRuntime lifetime is transferred to the DI container.")]
    internal static async Task<IServiceCollection> AddSquirixPersistenceServicesAsync(
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
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RecoveryService<object?>>>(),
            new RecoveryDependencies<object?>(
                sp.GetRequiredService<PersistenceOptions>(),
                sp.GetRequiredService<ManifestStore>(),
                sp.GetRequiredService<ILocalCacheRecovery<object?>>(),
                sp.GetRequiredService<JournalStartupGate>(),
                sp.GetRequiredService<RpcMutationIdempotencyStore>(),
                sp.GetRequiredService<ISnapshotReader>()),
            sp.GetService<Microsoft.Extensions.Hosting.IHostApplicationLifetime>()));
        _ = services.AddSingleton<SnapshotTriggerService<object?>>();
        _ = services.AddSingleton<ISnapshotReadinessStatus>(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());
        _ = services.AddSingleton(static sp => new JournalCompactionService<object?>(
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<JournalCompactionService<object?>>>(),
            sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<JournalCompactionOptions>>(),
            new JournalCompactionDependencies(
                sp.GetRequiredService<Coordinator>(),
                sp.GetRequiredService<IExclusiveMaintenanceExecutor>(),
                sp.GetRequiredService<ManifestStore>(),
                sp.GetRequiredService<ISnapshotReader>(),
                sp.GetRequiredService<PersistenceOptions>(),
                sp.GetRequiredService<ClusterConfig>())));
        _ = services.AddSingleton<IJournalCompactionStatus>(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddSingleton<JournalCompactionController>();
        _ = services.AddHostedService<JournalMetricsExporterService>();
    }

    private static void RegisterPersistenceRuntime(IServiceCollection services, PersistenceRuntime runtime)
    {
        _ = services.AddSingleton(runtime);
        _ = services.AddSingleton(runtime.Retention);
        _ = services.AddSingleton<IRetentionCleanupReadinessStatus>(runtime.Retention);
        _ = services.AddSingleton(runtime.ManifestStore);
        _ = services.AddSingleton(runtime.Gate);
        _ = services.AddSingleton(runtime.JournalCoordinator);
        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalCoordinatorDecorator(
            sp.GetRequiredService<JournalCoordinatorHost>().Coordinator,
            sp.GetRequiredService<IJournalOperationTracer>()));
        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<JournalCoordinatorHost>().Coordinator);
        _ = services.AddSingleton<IExclusiveMaintenanceExecutor>(static sp => sp.GetRequiredService<IJournalCoordinator>());
        _ = services.AddHealthChecks().AddCheck<JournalRecoveryReadinessHealthCheck>("journal_recovery", HealthStatus.Unhealthy, ["ready"])
                    .AddCheck<JournalMaintenanceReadinessHealthCheck>("journal_maintenance", HealthStatus.Unhealthy, ["ready"])
                    .AddCheck<RetentionCleanupReadinessCheck>("storage_retention_cleanup", HealthStatus.Unhealthy, ["ready"]);
        _ = services.AddSingleton<IJournalOperationTracer, OpenTelemetryJournalOperationTracer>();
        _ = services.AddSingleton<ISnapshotTelemetry, OpenTelemetrySnapshotTelemetry>();

        _ = services.AddSingleton<ISnapshotWriter>(static sp =>
        {
            var options = sp.GetRequiredService<PersistenceOptions>();
            return StoreFactory.CreateWriter(options);
        });
        _ = services.AddSingleton<ISnapshotReader>(static sp => StoreFactory.CreateReader(sp.GetRequiredService<PersistenceOptions>()));
        _ = services.AddSingleton<Coordinator>(static sp => new Coordinator(
            sp.GetRequiredService<TriggerOptions>(),
            sp.GetRequiredService<IJournalMetrics>(),
            new CoordinatorDependencies(
                sp.GetRequiredService<ISnapshotEntryCapture>(),
                sp.GetRequiredService<ISnapshotWriter>(),
                sp.GetRequiredService<ManifestStore>(),
                sp.GetRequiredService<IIdempotencySnapshotExporter>(),
                sp.GetRequiredService<ClusterConfig>().NodeId,
                sp.GetRequiredService<IBackgroundSnapshotMemoryThrottle>(),
                sp.GetRequiredService<ISnapshotTelemetry>())));
    }
}
