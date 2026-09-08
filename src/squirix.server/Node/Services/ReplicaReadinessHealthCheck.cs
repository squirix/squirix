using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness from read-only replica-group diagnostics.</summary>
/// <remarks>
/// A stale term or a leader fenced without majority contact reports not-ready. Every probe observes
/// the served groups into <see cref="ReplicationMetrics" />; diagnostics never mutate term, vote,
/// journal, or readiness state beyond reporting.
/// </remarks>
[Immutable]
internal sealed class ReplicaReadinessHealthCheck : IHealthCheck
{
    private readonly ReplicationMetrics _metrics;
    private readonly IReplicaStatusSource? _source;

    /// <summary>Initializes a new instance of the <see cref="ReplicaReadinessHealthCheck" /> class.</summary>
    /// <param name="source">The replica status source; <see langword="null" /> when replication is not configured.</param>
    /// <param name="metrics">The replication metrics observing each probe.</param>
    internal ReplicaReadinessHealthCheck(IReplicaStatusSource? source, ReplicationMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);
        _source = source;
        _metrics = metrics;
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        _ = context;
        if (_source == null)
            return HealthCheckResult.Healthy("replication is not configured.");

        var snapshots = await _source.GetSnapshotsAsync(cancellationToken).ConfigureAwait(false);
        if (snapshots.Count == 0)
            return HealthCheckResult.Healthy("no replica groups are served.");

        var failure = default(ReplicaStatusSnapshot?);
        var failureVerdict = ReplicaReadinessVerdict.Ready;
        for (var i = 0; i < snapshots.Count; i++)
        {
            var snapshot = snapshots[i];
            var verdict = ReplicaReadiness.Evaluate(in snapshot);
            _metrics.ReportGroup(in snapshot, verdict);
            if (verdict == ReplicaReadinessVerdict.Ready || failure != null)
                continue;
            failure = snapshot;
            failureVerdict = verdict;
        }

        if (failure == null)
            return HealthCheckResult.Healthy("replication is ready.");

        return HealthCheckResult.Unhealthy(ReplicaReadiness.Describe(failureVerdict, failure.Value.GroupId));
    }
}
