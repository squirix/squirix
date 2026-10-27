using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies the replication metrics only the leader of a group reports.</summary>
[Immutable]
public sealed class ReplicaLeaderMetricsTests : ServerUnitTestBase
{
    /// <summary>Verifies the leader reports the lag of each other copy, its write quorum and its authority, and they follow the next report.</summary>
    [Test]
    public async Task LeaderReportsOtherCopies()
    {
        using var meter = new Meter("Squirix");
        using var listener = CreateListener(meter, out var records);
        var metrics = new ReplicationMetrics(meter);
        var leading = new ReplicaStatusSnapshot("node-a", "group-a", 3, 4, 4, 10, 7, 7, true, true, true, true, true, GroupMemberState.Complete)
        {
            Role = ReplicaElectionRole.AuthorizedLeader,
            ReportsCopies = true,
            HasAuthority = true,
            WriteQuorumAvailable = true,
            Peers = [new ReplicaPeerStatus("node-b", "ready", 0), new ReplicaPeerStatus("node-c", "catching-up", 6)],
        };
        metrics.ReportGroup(in leading, ReplicaReadinessVerdict.Ready);
        listener.RecordObservableInstruments();

        await AssertValueAsync(records, "squirix_replication_lag_entries", "node-b", 0);
        await AssertValueAsync(records, "squirix_replication_lag_entries", "node-c", 6);
        await AssertValueAsync(records, "squirix_replication_quorum_available", null, 1);
        await AssertValueAsync(records, "squirix_replication_leader_authority", null, 1);

        records.Clear();
        var degraded = leading with
        {
            WriteQuorumAvailable = false,
            Peers = [new ReplicaPeerStatus("node-b", "catching-up", 3), new ReplicaPeerStatus("node-c", "catching-up", 6)],
        };
        metrics.ReportGroup(in degraded, ReplicaReadinessVerdict.Ready);
        listener.RecordObservableInstruments();

        await AssertValueAsync(records, "squirix_replication_lag_entries", "node-b", 3);
        await AssertValueAsync(records, "squirix_replication_quorum_available", null, 0);
    }

    /// <summary>Verifies a follower, and a leader whose coordinator does not run, report no peer series and no quorum, and authority zero.</summary>
    [Test]
    public async Task FollowerReportsNoPeerSeries()
    {
        using var meter = new Meter("Squirix");
        using var listener = CreateListener(meter, out var records);
        var metrics = new ReplicationMetrics(meter);
        var following = new ReplicaStatusSnapshot("node-a", "group-a", 3, 4, 4, 10, 7, 7, true, true, true, false, true, GroupMemberState.Complete);
        var promoting = new ReplicaStatusSnapshot("node-a", "group-b", 3, 4, 4, 10, 7, 7, true, true, true, false, true, GroupMemberState.Complete)
        {
            Role = ReplicaElectionRole.Leader,
        };
        metrics.ReportGroup(in following, ReplicaReadinessVerdict.Ready);
        metrics.ReportGroup(in promoting, ReplicaReadinessVerdict.Ready);
        listener.RecordObservableInstruments();

        _ = await Assert.That(Count(records, "squirix_replication_lag_entries", null)).IsEqualTo(0);
        _ = await Assert.That(Count(records, "squirix_replication_quorum_available", null)).IsEqualTo(0);
        _ = await Assert.That(Count(records, "squirix_replication_leader_authority", null)).IsEqualTo(2);
        _ = await Assert.That(Sum(records, "squirix_replication_leader_authority")).IsEqualTo(0.0);
    }

    /// <summary>Verifies a group reported as leader and then as follower leaves no peer or quorum series behind.</summary>
    [Test]
    public async Task SteppingDownRemovesLeaderSeries()
    {
        using var meter = new Meter("Squirix");
        using var listener = CreateListener(meter, out var records);
        var metrics = new ReplicationMetrics(meter);
        var leading = new ReplicaStatusSnapshot("node-a", "group-a", 3, 4, 4, 10, 7, 7, true, true, true, true, true, GroupMemberState.Complete)
        {
            Role = ReplicaElectionRole.AuthorizedLeader,
            ReportsCopies = true,
            HasAuthority = true,
            WriteQuorumAvailable = true,
            Peers = [new ReplicaPeerStatus("node-b", "ready", 0)],
        };
        metrics.ReportGroup(in leading, ReplicaReadinessVerdict.Ready);
        var following = new ReplicaStatusSnapshot("node-a", "group-a", 3, 5, 5, 10, 7, 7, true, true, true, false, true, GroupMemberState.Complete);
        metrics.ReportGroup(in following, ReplicaReadinessVerdict.Ready);
        listener.RecordObservableInstruments();

        _ = await Assert.That(Count(records, "squirix_replication_lag_entries", null)).IsEqualTo(0);
        _ = await Assert.That(Count(records, "squirix_replication_quorum_available", null)).IsEqualTo(0);
        await AssertValueAsync(records, "squirix_replication_leader_authority", null, 0);
    }

    /// <summary>Verifies an unknown commit is counted by the closed name of the record kind.</summary>
    [Test]
    public async Task CountsUnknownCommitsByKind()
    {
        using var meter = new Meter("Squirix");
        using var listener = CreateListener(meter, out var records);
        var metrics = new ReplicationMetrics(meter);

        metrics.ReportUnknownCommit(GroupRecordKind.UserMutation);
        metrics.ReportUnknownCommit(GroupRecordKind.UserMutation);
        metrics.ReportUnknownCommit(GroupRecordKind.Expiration);

        _ = await Assert.That(Count(records, "squirix_replication_unknown_commit_total", "user_mutation")).IsEqualTo(2);
        _ = await Assert.That(Count(records, "squirix_replication_unknown_commit_total", "expiration")).IsEqualTo(1);
    }

    private static int Count(List<Record> records, string name, string? label)
    {
        var count = 0;
        for (var i = 0; i < records.Count; i++)
        {
            if (string.Equals(records[i].Name, name, StringComparison.Ordinal) && (label == null || string.Equals(records[i].Label, label, StringComparison.Ordinal)))
                count++;
        }

        return count;
    }

    private static double Sum(List<Record> records, string name)
    {
        var sum = 0.0;
        for (var i = 0; i < records.Count; i++)
        {
            if (string.Equals(records[i].Name, name, StringComparison.Ordinal))
                sum += records[i].Value;
        }

        return sum;
    }

    private static async Task AssertValueAsync(List<Record> records, string name, string? label, double expected)
    {
        var found = false;
        for (var i = 0; i < records.Count; i++)
        {
            if (!string.Equals(records[i].Name, name, StringComparison.Ordinal) || (label != null && !string.Equals(records[i].Label, label, StringComparison.Ordinal)))
                continue;

            _ = await Assert.That(records[i].Group).IsEqualTo("group-a");
            _ = await Assert.That(records[i].Value).IsEqualTo(expected);
            found = true;
        }

        _ = await Assert.That(found).IsTrue().Because($"Expected a measurement of '{name}'.");
    }

    private static MeterListener CreateListener(Meter meter, out List<Record> records)
    {
        var captured = new List<Record>();
        records = captured;
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, target) =>
            {
                if (ReferenceEquals(instrument.Meter, meter))
                    target.EnableMeasurementEvents(instrument, captured);
            },
        };
        listener.SetMeasurementEventCallback<long>(static (instrument, value, tags, state) => Capture(state, instrument.Name, value, tags));
        listener.SetMeasurementEventCallback<int>(static (instrument, value, tags, state) => Capture(state, instrument.Name, value, tags));
        listener.Start();
        return listener;
    }

    private static void Capture(object? state, string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (state is not List<Record> captured)
            return;

        string? group = null;
        string? label = null;
        for (var i = 0; i < tags.Length; i++)
        {
            if (string.Equals(tags[i].Key, "group", StringComparison.Ordinal))
                group = tags[i].Value as string;
            else if (string.Equals(tags[i].Key, "peer", StringComparison.Ordinal) || string.Equals(tags[i].Key, "operation", StringComparison.Ordinal))
                label = tags[i].Value as string;
        }

        captured.Add(new Record(name, value, group, label));
    }

    private sealed class Record
    {
        internal Record(string name, double value, string? group, string? label)
        {
            Name = name;
            Value = value;
            Group = group;
            Label = label;
        }

        internal string? Group { get; }

        internal string? Label { get; }

        internal string Name { get; }

        internal double Value { get; }
    }
}
