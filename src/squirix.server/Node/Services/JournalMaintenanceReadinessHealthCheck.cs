using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness based on fatal journal maintenance failures.</summary>
[Immutable]
internal sealed class JournalMaintenanceReadinessHealthCheck : IHealthCheck
{
    private readonly IJournalCompactionStatus _compaction;
    private readonly IJournalCoordinator _journal;
    private readonly ISnapshotReadinessStatus _snapshot;

    internal JournalMaintenanceReadinessHealthCheck(IJournalCoordinator journal, IJournalCompactionStatus compaction, ISnapshotReadinessStatus snapshot)
    {
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(compaction);
        ArgumentNullException.ThrowIfNull(snapshot);
        _journal = journal;
        _compaction = compaction;
        _snapshot = snapshot;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        // A latched pipeline (failed fsync/flush, segment roll, maintenance, or post-ring apply) cannot commit
        // until restart. Report the reason as type and message only: no exception, so no stack trace in the report.
        if (_journal.GetJournalThreadFailure() is { } failure)
            return Task.FromResult(HealthCheckResult.Unhealthy($"journal pipeline is latched as failed until restart: {failure.GetType().Name}: {failure.Message}"));

        if (_compaction.State is RunState.Failed)
            return Task.FromResult(HealthCheckResult.Unhealthy("journal compaction is in failed state."));

        var healthy = _snapshot.HasFatalFailure ? HealthCheckResult.Unhealthy("Snapshot trigger service has a fatal failure.")
            : HealthCheckResult.Healthy("journal maintenance is ready.");
        return Task.FromResult(healthy);
    }
}
