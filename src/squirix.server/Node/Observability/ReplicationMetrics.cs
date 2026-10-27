using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Observability;

/// <summary>Stable low-cardinality replication metrics on the host-scoped <see cref="Meter" />.</summary>
/// <remarks>
/// Labels stay bounded: node and group identifiers only, plus closed reason and scope values.
/// Observable gauges report the last observed per-group snapshot; each mismatch reason fires only on
/// its own transition into mismatch so repeated read-path reports never inflate the series.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicationMetrics
{
    private const string GroupTag = "group";
    private const string IndexUnit = "{index}";

    private readonly Counter<long> _compactionsTotal;
    private readonly Counter<long> _compactionSkippedTotal;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, GroupObservation> _groups = [with(StringComparer.Ordinal)];
    private readonly Counter<long> _idempotencyRefusalsTotal;
    private readonly Counter<long> _inconsistentRecordsTotal;
    private readonly Counter<long> _mismatchTotal;
    private readonly Counter1Label _reportsTotal;
    private readonly Counter1Label _unknownCommitsTotal;

    internal ReplicationMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _reportsTotal = new Counter1Label(meter.CreateCounter<long>("squirix_replication_status_reports_total", "{report}", "Replica status read-path reports"), "node");
        _unknownCommitsTotal = new Counter1Label(
            meter.CreateCounter<long>("squirix_replication_unknown_commit_total", "{commit}", "Commits that ended with an unknown outcome after the local append, by record kind"),
            "operation");
        _mismatchTotal = meter.CreateCounter<long>("squirix_replication_topology_mismatch_total", "{mismatch}", "Replica topology identity mismatches observed on the read path");
        _compactionsTotal = meter.CreateCounter<long>("squirix_replication_log_compactions_total", "{compaction}", "Compactions of the replica group log this node owns, by outcome");
        _compactionSkippedTotal = meter.CreateCounter<long>(
            "squirix_replication_log_compaction_skipped_total",
            "{compaction}",
            "Passes that found the owned replica group log past a compaction threshold but did not compact it, by reason");

        _inconsistentRecordsTotal = meter.CreateCounter<long>(
            "squirix_replication_inconsistent_records_total",
            "{record}",
            "Replica group log records refused because their effect contradicts their outcome or they cannot be decoded");

        _idempotencyRefusalsTotal = meter.CreateCounter<long>(
            "squirix_replication_idempotency_refusals_total",
            "{refusal}",
            "New operations a led replica group refused retryably because its idempotency state was full of outcomes that could not be given up yet");

        _ = meter.CreateObservableGauge("squirix_replication_term", ObserveTerms, description: "Current term observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_commit_index", ObserveCommitIndexes, IndexUnit, "Durable commit index observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_applied_index", ObserveAppliedIndexes, IndexUnit, "Index last applied to memory observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_commit_lag_entries", ObserveCommitLags, IndexUnit, "Durable entries past the commit index observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_apply_lag_entries", ObserveApplyLags, IndexUnit, "Committed entries not yet applied observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_topology_match", () => ObserveFlags(static observation => observation.TopologyMatch), description: "Topology fingerprint agreement as 1=match, 0=mismatch");
        _ = meter.CreateObservableGauge("squirix_replication_generation_match", () => ObserveFlags(static observation => observation.GenerationMatch), description: "Configuration generation agreement as 1=match, 0=mismatch");
        _ = meter.CreateObservableGauge("squirix_replication_ready", () => ObserveFlags(static observation => observation.Ready), description: "Replica group readiness as 1=ready, 0=not ready");
        _ = meter.CreateObservableGauge("squirix_replication_log_bytes", ObserveLogBytes, "By", "Durable size of the replica group log file observed by the replica group log");
        _ = meter.CreateObservableGauge(
            "squirix_replication_log_retained_entries",
            ObserveRetainedEntries,
            "{entry}",
            "Entries retained in the replica group log file observed by the replica group log");
        _ = meter.CreateObservableGauge("squirix_replication_snapshot_index", ObserveSnapshotIndexes, IndexUnit, "Last log index the published replica group snapshot covers");
        _ = meter.CreateObservableCounter(
            "squirix_replication_idempotency_capacity_evictions_total",
            () => ObserveIdempotency(static observation => observation.IdempotencyCapacityEvictions),
            "{outcome}",
            "Group idempotency outcomes this node gave up to make room before they expired, since the group log was opened");
        _ = meter.CreateObservableGauge(
            "squirix_replication_idempotency_outcome_bytes",
            () => ObserveIdempotency(static observation => observation.IdempotencyOutcomeBytes),
            "By",
            "Bytes the group idempotency outcomes and in-flight operations are charged against the outcome byte bound");
        LeaderView.Register(meter, SnapshotGroups);
        _ = meter.CreateObservableGauge(
            "squirix_replication_role",
            ObserveRoles,
            description: "Election role in the replica group as 0=follower, 1=pre-candidate, 2=candidate, 3=leader without authority, 4=leader with authority");
    }

    /// <summary>Counts one compaction of the replica group log this node owns.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The compacted replica group identifier.</param>
    /// <param name="outcome">The closed compaction outcome name: compacted, or compacted_past_follower when a follower was left behind.</param>
    internal void ReportCompaction(string nodeId, string groupId, string outcome)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { GroupTag, groupId },
            { "outcome", outcome },
        };
        _compactionsTotal.Add(1, in tags);
    }

    /// <summary>Counts one new operation a led replica group refused retryably because its idempotency state was full.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    internal void ReportIdempotencyRefusal(string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { "group", groupId },
        };
        _idempotencyRefusalsTotal.Add(1, in tags);
    }

    /// <summary>Counts one replica group log record refused as inconsistent before it touched memory.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    internal void ReportInconsistentRecord(string nodeId, string groupId)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { GroupTag, groupId },
        };
        _inconsistentRecordsTotal.Add(1, in tags);
    }

    /// <summary>Counts one commit that ended with an unknown outcome after the local append.</summary>
    /// <param name="kind">The kind of the record the commit carried.</param>
    internal void ReportUnknownCommit(GroupRecordKind kind) => _unknownCommitsTotal.WithLabels(LeaderView.OperationName(kind)).Inc(1);

    /// <summary>Counts one pass that found the owned replica group log past a threshold but did not compact it.</summary>
    /// <param name="nodeId">The observing node identifier.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="reason">The closed compaction outcome name that stopped the compaction.</param>
    internal void ReportCompactionSkipped(string nodeId, string groupId, string reason)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { GroupTag, groupId },
            { "reason", reason },
        };
        _compactionSkippedTotal.Add(1, in tags);
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
            verdict == ReplicaReadinessVerdict.Ready,
            new GroupRetention(snapshot.LogBytes, snapshot.RetainedEntries, long.CreateSaturating(snapshot.SnapshotIndex)))
        {
            Role = snapshot.Role,
            HasAuthority = snapshot.HasAuthority,
            Leads = snapshot.ReportsCopies,
            WriteQuorumAvailable = snapshot.WriteQuorumAvailable,
            Peers = snapshot.Peers,
            IdempotencyCapacityEvictions = snapshot.IdempotencyCapacityEvictions,
            IdempotencyOutcomeBytes = snapshot.IdempotencyOutcomeBytes,
        };

        var (topologyRaised, generationRaised) = GetAndStoreTransitions(snapshot.GroupId, in observation);
        if (topologyRaised)
            AddMismatch(snapshot.NodeId, snapshot.GroupId, "topology");
        if (generationRaised)
            AddMismatch(snapshot.NodeId, snapshot.GroupId, "generation");
    }

    private void AddMismatch(string nodeId, string groupId, string reason)
    {
        var tags = new TagList
        {
            { "node", nodeId },
            { GroupTag, groupId },
            { "reason", reason },
        };
        _mismatchTotal.Add(1, in tags);
    }

    private (bool TopologyRaised, bool GenerationRaised) GetAndStoreTransitions(string groupId, in GroupObservation observation)
    {
        lock (_gate)
        {
            var raiseTopology = !observation.TopologyMatch;
            var raiseGeneration = !observation.GenerationMatch;
            if (_groups.TryGetValue(groupId, out var previous))
            {
                raiseTopology = raiseTopology && previous.TopologyMatch;
                raiseGeneration = raiseGeneration && previous.GenerationMatch;
            }

            _groups[groupId] = observation with { GroupId = groupId };
            return (raiseTopology, raiseGeneration);
        }
    }

    private IEnumerable<Measurement<long>> ObserveAppliedIndexes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].AppliedIndex, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveApplyLags()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].ApplyLag, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveCommitIndexes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].CommitIndex, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveCommitLags()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].CommitLag, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveFlags(Func<GroupObservation, bool> select)
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(select(snapshot[i]) ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveIdempotency(Func<GroupObservation, long> select)
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(select(snapshot[i]), snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveLogBytes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].Retention.LogBytes, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveRetainedEntries()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].Retention.RetainedEntries, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<int>> ObserveRoles()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(ReplicaGroupMeasurements.RoleValue(snapshot[i].Role), snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveSnapshotIndexes()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].Retention.SnapshotIndex, snapshot[i].NodeId, snapshot[i].GroupId);
    }

    private IEnumerable<Measurement<long>> ObserveTerms()
    {
        var snapshot = SnapshotGroups();
        for (var i = 0; i < snapshot.Length; i++)
            yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].Term, snapshot[i].NodeId, snapshot[i].GroupId);
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
        bool Ready,
        GroupRetention Retention)
    {
        internal string GroupId { get; init; } = string.Empty;

        internal bool HasAuthority { get; init; }

        internal long IdempotencyCapacityEvictions { get; init; }

        internal long IdempotencyOutcomeBytes { get; init; }

        internal bool Leads { get; init; }

        internal IReadOnlyList<ReplicaPeerStatus> Peers { get; init; } = [];

        internal ReplicaElectionRole Role { get; init; }

        internal bool WriteQuorumAvailable { get; init; }
    }

    /// <summary>The retained size of an observed replica group log.</summary>
    /// <param name="LogBytes">The durable size of the group log file in bytes.</param>
    /// <param name="RetainedEntries">The number of entry frames the group log file holds.</param>
    /// <param name="SnapshotIndex">The last log index the published snapshot covers.</param>
    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct GroupRetention(long LogBytes, int RetainedEntries, long SnapshotIndex);

    [Immutable]
    private sealed record Counter1Label(Counter<long> Counter, string Key1)
    {
        internal ServerCounterLabelBinding WithLabels(string v1) => new(Counter, Key1, v1, "scope", "replication");
    }

    /// <summary>The metrics only the leader of a group reports: its view of the other copies, its write quorum and authority.</summary>
    private static class LeaderView
    {
        internal static void Register(Meter meter, Func<GroupObservation[]> snapshot)
        {
            _ = meter.CreateObservableGauge(
                "squirix_replication_lag_entries",
                () => ObservePeerLags(snapshot()),
                IndexUnit,
                "Entries each other copy of a group this node leads is behind the leader last log index, by the match index the leader knows");
            _ = meter.CreateObservableGauge(
                "squirix_replication_quorum_available",
                () => ObserveLedFlags(snapshot(), static observation => observation.WriteQuorumAvailable),
                description: "Whether a current majority counts toward the write quorum of a group this node leads, as 1=available, 0=unavailable");
            _ = meter.CreateObservableGauge(
                "squirix_replication_leader_authority",
                () => ObserveAuthority(snapshot()),
                description: "Whether this node leads the replica group and holds write authority, as 1=holds, 0=does not");
        }

        internal static string OperationName(GroupRecordKind kind) => kind switch
        {
            GroupRecordKind.UserMutation => "user_mutation",
            GroupRecordKind.Expiration => "expiration",
            GroupRecordKind.Metadata => "metadata",
            GroupRecordKind.LeaderTerm => "leader_term",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported record kind."),
        };

        private static IEnumerable<Measurement<int>> ObserveAuthority(GroupObservation[] snapshot)
        {
            for (var i = 0; i < snapshot.Length; i++)
                yield return ReplicaGroupMeasurements.MeasureNodeGroup(snapshot[i].HasAuthority ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
        }

        private static IEnumerable<Measurement<int>> ObserveLedFlags(GroupObservation[] snapshot, Func<GroupObservation, bool> select)
        {
            for (var i = 0; i < snapshot.Length; i++)
            {
                if (snapshot[i].Leads)
                    yield return ReplicaGroupMeasurements.MeasureNodeGroup(select(snapshot[i]) ? 1 : 0, snapshot[i].NodeId, snapshot[i].GroupId);
            }
        }

        private static IEnumerable<Measurement<long>> ObservePeerLags(GroupObservation[] snapshot)
        {
            for (var i = 0; i < snapshot.Length; i++)
            {
                if (!snapshot[i].Leads)
                    continue;

                var peers = snapshot[i].Peers;
                for (var peer = 0; peer < peers.Count; peer++)
                {
                    var tags = new TagList
                    {
                        { "node", snapshot[i].NodeId },
                        { GroupTag, snapshot[i].GroupId },
                        { "peer", peers[peer].PeerId },
                    };
                    yield return new Measurement<long>(long.CreateSaturating(peers[peer].LagEntries), in tags);
                }
            }
        }
    }
}
