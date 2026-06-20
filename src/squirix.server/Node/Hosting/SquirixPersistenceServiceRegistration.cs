using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixPersistenceServiceRegistration
{
    public static async Task<IServiceCollection> AddSquirixPersistenceServicesAsync(
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

    private static void RegisterPersistenceRuntime(IServiceCollection services, PersistenceRuntime runtime)
    {
        _ = services.AddSingleton(runtime);
        _ = services.AddSingleton(runtime.Retention);
        _ = services.AddSingleton<IRetentionCleanupReadinessStatus>(runtime.Retention);
        _ = services.AddSingleton(runtime.ManifestStore);
        _ = services.AddSingleton(runtime.Gate);
        _ = services.AddSingleton(runtime.JournalCoordinator);
        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalWriterDecorator(
            sp.GetRequiredService<JournalCoordinatorHost>().Coordinator,
            sp.GetRequiredService<IJournalOperationTracer>()));
        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<JournalCoordinatorHost>().Coordinator);
        _ = services.AddSingleton<IExclusiveMaintenanceExecutor>(static sp => sp.GetRequiredService<IJournalCoordinator>());
        _ = services.AddHealthChecks().AddCheck<JournalRecoveryReadinessHealthCheck>("journal_recovery", HealthStatus.Unhealthy, ["ready"])
                    .AddCheck<JournalMaintenanceReadinessHealthCheck>("journal_maintenance", HealthStatus.Unhealthy, ["ready"])
                    .AddCheck<StorageRetentionCleanupReadinessHealthCheck>("storage_retention_cleanup", HealthStatus.Unhealthy, ["ready"]);
        _ = services.AddSingleton<IJournalOperationTracer, OpenTelemetryJournalOperationTracer>();

        _ = services.AddSingleton<ISnapshotWriter>(static sp => new SnapshotWriter(sp.GetRequiredService<PersistenceOptions>().DataDir));
        _ = services.AddSingleton<SnapshotReader>();
        _ = services.AddSingleton<SnapshotCoordinator<object?>>();
    }

    private static void RegisterPersistenceHostedServices(IServiceCollection services, bool waitForRecovery)
    {
        _ = services.AddSingleton(new RecoveryOptions { BlockOnStart = waitForRecovery });
        _ = services.AddHostedService<RecoveryService<object?>>();
        _ = services.AddSingleton<SnapshotTriggerService<object?>>();
        _ = services.AddSingleton<ISnapshotReadinessStatus>(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<SnapshotTriggerService<object?>>());
        _ = services.AddSingleton<JournalCompactionService<object?>>();
        _ = services.AddSingleton<IJournalCompactionStatus>(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddHostedService(static sp => sp.GetRequiredService<JournalCompactionService<object?>>());
        _ = services.AddSingleton<JournalCompactionController>();
        _ = services.AddHostedService<JournalMetricsExporterService>();
    }
}
