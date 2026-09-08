using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies stable replication instruments with bounded label sets.</summary>
[Immutable]
public sealed class ReplicationMetricsTests : ServerUnitTestBase
{
    /// <summary>Verifies instrument names are stable and labels stay bounded to node, group, scope, and reason.</summary>
    [Fact]
    public void ExposesStableMetricsWithBoundedLabels()
    {
        using var meter = new Meter("Squirix");
        using var listener = CreateListener(meter, out var records);
        var metrics = new ReplicationMetrics(meter);

        var ready = new ReplicaStatusSnapshot("node-a", "group-a", 3, 4, 4, 10, 7, 5, true, true, true, true, true);
        metrics.ReportGroup(in ready, ReplicaReadinessVerdict.Ready);

        var topologyMismatch = new ReplicaStatusSnapshot("node-a", "group-b", 3, 6, 6, 3, 3, 3, false, true, true, true, true);
        metrics.ReportGroup(in topologyMismatch, ReplicaReadinessVerdict.TopologyMismatch);
        metrics.ReportGroup(in topologyMismatch, ReplicaReadinessVerdict.TopologyMismatch);

        var generationMismatch = new ReplicaStatusSnapshot("node-a", "group-c", 3, 6, 6, 3, 3, 3, true, false, true, true, true);
        metrics.ReportGroup(in generationMismatch, ReplicaReadinessVerdict.TopologyMismatch);

        // A staggered mismatch raises each reason exactly once on its own transition.
        var staggeredTopology = new ReplicaStatusSnapshot("node-a", "group-d", 3, 6, 6, 3, 3, 3, false, true, true, true, true);
        metrics.ReportGroup(in staggeredTopology, ReplicaReadinessVerdict.TopologyMismatch);
        var staggeredBoth = new ReplicaStatusSnapshot("node-a", "group-d", 3, 6, 6, 3, 3, 3, false, false, true, true, true);
        metrics.ReportGroup(in staggeredBoth, ReplicaReadinessVerdict.TopologyMismatch);
        metrics.ReportGroup(in staggeredBoth, ReplicaReadinessVerdict.TopologyMismatch);

        listener.RecordObservableInstruments();

        AssertReportsTotal(records, 7);
        AssertMismatchTotal(records, "group-b", "topology", 1);
        AssertMismatchTotal(records, "group-c", "generation", 1);
        AssertMismatchTotal(records, "group-d", "topology", 1);
        AssertMismatchTotal(records, "group-d", "generation", 1);
        AssertGauge(records, "squirix_replication_term", "group-a", 4);
        AssertGauge(records, "squirix_replication_commit_index", "group-a", 7);
        AssertGauge(records, "squirix_replication_applied_index", "group-a", 5);
        AssertGauge(records, "squirix_replication_commit_lag_entries", "group-a", 3);
        AssertGauge(records, "squirix_replication_apply_lag_entries", "group-a", 2);
        AssertGauge(records, "squirix_replication_topology_match", "group-a", 1);
        AssertGauge(records, "squirix_replication_generation_match", "group-a", 1);
        AssertGauge(records, "squirix_replication_ready", "group-a", 1);
        AssertGauge(records, "squirix_replication_term", "group-b", 6);
        AssertGauge(records, "squirix_replication_topology_match", "group-b", 0);
        AssertGauge(records, "squirix_replication_ready", "group-b", 0);
        AssertLabelSetsAreBounded(records);
    }

    private static void AssertGauge(List<MeasurementRecord> records, string name, string group, double expected)
    {
        var found = false;
        for (var i = 0; i < records.Count; i++)
        {
            if (!string.Equals(records[i].Name, name, StringComparison.Ordinal) || !string.Equals(records[i].Group, group, StringComparison.Ordinal))
                continue;

            Assert.Equal(expected, records[i].Value);
            found = true;
        }

        Assert.True(found, $"Expected gauge '{name}' for group '{group}'.");
    }

    private static void AssertLabelSetsAreBounded(List<MeasurementRecord> records)
    {
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            Assert.StartsWith("squirix_replication_", record.Name, StringComparison.Ordinal);
            Assert.True(record.Scope == null || string.Equals(record.Scope, "replication", StringComparison.Ordinal));
            Assert.True(
                record.Reason == null || string.Equals(record.Reason, "topology", StringComparison.Ordinal) ||
                string.Equals(record.Reason, "generation", StringComparison.Ordinal));
            Assert.True(record.Node != null || record.Group != null);
        }
    }

    private static void AssertMismatchTotal(List<MeasurementRecord> records, string group, string reason, int expectedCount)
    {
        var count = 0;
        for (var i = 0; i < records.Count; i++)
        {
            if (!string.Equals(records[i].Name, "squirix_replication_topology_mismatch_total", StringComparison.Ordinal))
                continue;

            if (string.Equals(records[i].Group, group, StringComparison.Ordinal) && string.Equals(records[i].Reason, reason, StringComparison.Ordinal))
                count++;
        }

        Assert.Equal(expectedCount, count);
    }

    private static void AssertReportsTotal(List<MeasurementRecord> records, int expectedCount)
    {
        var count = 0;
        for (var i = 0; i < records.Count; i++)
        {
            if (!string.Equals(records[i].Name, "squirix_replication_status_reports_total", StringComparison.Ordinal))
                continue;

            Assert.Equal("node-a", records[i].Node);
            Assert.Equal("replication", records[i].Scope);
            count++;
        }

        Assert.Equal(expectedCount, count);
    }

    private static MeterListener CreateListener(Meter meter, out List<MeasurementRecord> records)
    {
        var captured = new List<MeasurementRecord>();
        records = captured;
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, target) =>
            {
                if (ReferenceEquals(instrument.Meter, meter))
                    target.EnableMeasurementEvents(instrument, captured);
            },
        };
        listener.SetMeasurementEventCallback<long>(static (instrument, value, tags, state) => Record(state, instrument.Name, value, tags));
        listener.SetMeasurementEventCallback<int>(static (instrument, value, tags, state) => Record(state, instrument.Name, value, tags));
        listener.Start();
        return listener;
    }

    private static string? FindTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            if (string.Equals(tags[i].Key, key, StringComparison.Ordinal))
                return tags[i].Value as string;
        }

        return null;
    }

    private static void Record(object? state, string name, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (state is List<MeasurementRecord> captured)
            captured.Add(new MeasurementRecord(name, value, FindTag(tags, "node"), FindTag(tags, "group"), FindTag(tags, "scope"), FindTag(tags, "reason")));
    }

    private static void Record(object? state, string name, int value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (state is List<MeasurementRecord> captured)
            captured.Add(new MeasurementRecord(name, value, FindTag(tags, "node"), FindTag(tags, "group"), FindTag(tags, "scope"), FindTag(tags, "reason")));
    }

    private sealed class MeasurementRecord
    {
        internal MeasurementRecord(string name, double value, string? node, string? group, string? scope, string? reason)
        {
            Name = name;
            Value = value;
            Node = node;
            Group = group;
            Scope = scope;
            Reason = reason;
        }

        internal string? Group { get; }

        internal string Name { get; }

        internal string? Node { get; }

        internal string? Reason { get; }

        internal string? Scope { get; }

        internal double Value { get; }
    }
}
