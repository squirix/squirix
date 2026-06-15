using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixPersistenceServiceRegistration
{
    public static IServiceCollection AddSquirixPersistenceServices(this IServiceCollection services, bool waitForRecovery)
    {
        _ = services.AddSingleton(static sp => new ManifestStore(sp.GetRequiredService<PersistenceOptions>(), sp.GetRequiredService<ILogger<ManifestStore>>()));
        _ = services.AddSingleton(static _ => new JournalStartupGate(false));
        _ = services.AddHealthChecks().AddCheck<JournalRecoveryReadinessHealthCheck>("journal_recovery", HealthStatus.Unhealthy, ["ready"])
                    .AddCheck<JournalMaintenanceReadinessHealthCheck>("journal_maintenance", HealthStatus.Unhealthy, ["ready"]);
        _ = services.AddSingleton<IJournalOperationTracer, OpenTelemetryJournalOperationTracer>();
        _ = services.AddSingleton(static sp =>
        {
            var persistence = sp.GetRequiredService<PersistenceOptions>();
            var ms = sp.GetRequiredService<ManifestStore>();
            var manifest = ms.ReadCurrentOrDefault();
            return new JournalWriter(persistence, manifest, ms, sp.GetRequiredService<JournalStartupGate>());
        });
        _ = services.AddSingleton<IJournalCoordinator>(static sp => new TracingJournalWriterDecorator(
            sp.GetRequiredService<JournalWriter>(),
            sp.GetRequiredService<IJournalOperationTracer>()));
        _ = services.AddSingleton<IJournalMetrics>(static sp => sp.GetRequiredService<JournalWriter>());
        _ = services.AddSingleton<IExclusiveMaintenanceExecutor>(static sp => sp.GetRequiredService<IJournalCoordinator>());

        _ = services.AddSingleton<ISnapshotWriter>(static sp => new SnapshotWriter(sp.GetRequiredService<PersistenceOptions>().DataDir));
        _ = services.AddSingleton<SnapshotReader>();

        _ = services.AddSingleton<SnapshotCoordinator<object?>>();

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

        return services;
    }
}
