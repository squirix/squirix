using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Reports readiness based on fatal journal maintenance failures.
/// </summary>
internal sealed class JournalMaintenanceReadinessHealthCheck : IHealthCheck
{
    private readonly IJournalCompactionStatus _compaction;
    private readonly IJournalCoordinator _journal;
    private readonly ISnapshotReadinessStatus _snapshot;

    public JournalMaintenanceReadinessHealthCheck(IJournalCoordinator journal, IJournalCompactionStatus compaction, ISnapshotReadinessStatus snapshot)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _compaction = compaction ?? throw new ArgumentNullException(nameof(compaction));
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        _ = context;
        _ = cancellationToken;

        if (_journal.HasFlushLoopFailure)
            return Task.FromResult(HealthCheckResult.Unhealthy("journal periodic flush loop failed."));

        if (_compaction.State == CompactionState.Failed)
            return Task.FromResult(HealthCheckResult.Unhealthy("journal compaction is in failed state."));

        var healthy = _snapshot.HasFatalFailure
            ? HealthCheckResult.Unhealthy("Snapshot trigger service has a fatal failure.")
            : HealthCheckResult.Healthy("journal maintenance is ready.");
        return Task.FromResult(healthy);
    }
}
