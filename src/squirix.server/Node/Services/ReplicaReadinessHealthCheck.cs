using System;
using System.Threading;
using System.Threading.Tasks;
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
internal sealed class ReplicaReadinessHealthCheck : IReadinessCheck
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
    public async Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken)
    {
        if (_source == null)
            return ReadinessResult.Healthy("replication is not configured.");

        var snapshots = await _source.GetSnapshotsAsync(cancellationToken).ConfigureAwait(false);
        if (snapshots.Count == 0)
            return ReadinessResult.Healthy("no replica groups are served.");

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

        return failure == null ? ReadinessResult.Healthy("replication is ready.") : ReadinessResult.Unhealthy(ReplicaReadiness.Describe(failureVerdict, failure.Value.GroupId));
    }
}
