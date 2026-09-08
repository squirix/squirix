using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Stable low-cardinality replication metrics on the host-scoped <see cref="Meter" />.</summary>
/// <remarks>
/// Labels stay bounded: node and group identifiers only, plus closed reason and scope values.
/// Observable gauges report the last observed per-group snapshot; mismatch counters fire only on the
/// transition into mismatch so repeated read-path reports never inflate the series.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicationMetrics
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, GroupObservation> _groups = new(StringComparer.Ordinal);
    private readonly Counter2Labels _mismatchTotal;
    private readonly Counter1Label _reportsTotal;

    internal ReplicationMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _reportsTotal = new Counter1Label(meter.CreateCounter<long>("squirix_replication_status_reports_total", "{report}", "Replica status read-path reports"), "node");
        _mismatchTotal = new Counter2Labels(
            meter.CreateCounter<long>("squirix_replication_topology_mismatch_total", "{mismatch}", "Replica topology identity mismatches observed on the read path"),
            "group",
            "reason");

        _ = meter.CreateObservableGauge("squirix_replication_term", ObserveTerms, description: "Current term observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_commit_index", ObserveCommitIndexes, "{index}", "Durable commit index observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_applied_index", ObserveAppliedIndexes, "{index}", "Index last applied to memory observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_commit_lag_entries", ObserveCommitLags, "{index}", "Durable entries past the commit index observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_apply_lag_entries", ObserveApplyLags, "{index}", "Committed entries not yet applied observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_topology_match", ObserveTopologyMatches, description: "Topology fingerprint agreement as 1=match, 0=mismatch");
        _ = meter.CreateObservableGauge("squirix_replication_generation_match", ObserveGenerationMatches, description: "Configuration generation agreement as 1=match, 0=mismatch");
        _ = meter.CreateObservableGauge("squirix_replication_ready", ObserveReady, description: "Replica group readiness as 1=ready, 0=not ready");
    }

    /// <summary>Observes one replica-group snapshot on a read path without mutating replication state.</summary>
    /// <param name="snapshot">The observed replica-group status.</param>
    /// <param name="verdict">The evaluated readiness verdict.</param>
    internal void ReportGroup(in ReplicaStatusSnapshot snapshot, ReplicaReadinessVerdict verdict)
    {
        _reportsTotal.WithLabels(snapshot.NodeId).Inc(1);

        var observation = new GroupObservation(
            snapshot.NodeId,
            long.CreateSaturating(snapshot.CurrentTerm),
            long.CreateSaturating(snapshot.CommitIndex),
            long.CreateSaturating(snapshot.LastAppliedIndex),
            long.CreateSaturating(snapshot.LastLogIndex >= snapshot.CommitIndex ? snapshot.LastLogIndex - snapshot.CommitIndex : 0UL),
            long.CreateSaturating(snapshot.CommitIndex >= snapshot.LastAppliedIndex ? snapshot.CommitIndex - snapshot.LastAppliedIndex : 0UL),
            snapshot.FingerprintMatch,
            snapshot.GenerationMatch,
            verdict == ReplicaReadinessVerdict.Ready);

        var isNewMismatch = GetAndStoreObservation(snapshot.GroupId, observation);
        var raiseMismatch = observation.IsMismatch && isNewMismatch;

        if (!raiseMismatch)
            return;

        if (!observation.TopologyMatch)
            _mismatchTotal.WithLabels(snapshot.GroupId, "topology").Inc(1);
        if (!observation.GenerationMatch)
            _mismatchTotal.WithLabels(snapshot.GroupId, "generation").Inc(1);
    }

    private static Measurement<long> MeasureNodeGroup(long value, string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        return new Measurement<long>(value, in tags);
    }

    private static Measurement<int> MeasureNodeGroup(int value, string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        return new Measurement<int>(value, in tags);
    }

    private bool GetAndStoreObservation(string groupId, GroupObservation observation)
    {
        lock (_gate)
        {
            var raise = !_groups.TryGetValue(groupId, out var previous) || !previous.IsMismatch;
            _groups[groupId] = observation with { GroupId = groupId };
            return raise;
        }
    }

    private IEnumerable<Measurement<long>> ObserveAppliedIndexes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].AppliedIndex, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveApplyLags()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].ApplyLag, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveCommitIndexes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].CommitIndex, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveCommitLags()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].CommitLag, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveGenerationMatches()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].GenerationMatch ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveReady()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].Ready ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveTerms()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].Term, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveTopologyMatches()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return MeasureNodeGroup(snapshot[i].TopologyMatch ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private GroupObservation[] SnapshotGroups()
    {
        lock (_gate)
        {
            var snapshot = new GroupObservation[_groups.Count];
            var index = 0;
            foreach (var pair in _groups)
            {
                snapshot[index] = pair.Value;
                index++;
            }

            return snapshot;
        }
    }

    [Immutable]
    private readonly record struct GroupObservation(
        string NodeId,
        long Term,
        long CommitIndex,
        long AppliedIndex,
        long CommitLag,
        long ApplyLag,
        bool TopologyMatch,
        bool GenerationMatch,
        bool Ready)
    {
        internal string GroupId { get; init; } = string.Empty;

        internal bool IsMismatch => !TopologyMatch || !GenerationMatch;
    }

    [Immutable]
    private sealed record Counter1Label(Counter<long> Counter, string Key1)
    {
        internal ServerCounterLabelBinding WithLabels(string v1) => new(Counter, Key1, v1, "scope", "replication");
    }

    [Immutable]
    private sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
    {
        internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
    }
}
