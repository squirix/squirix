using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Observability;

/// <summary>OpenTelemetry-backed <see cref="ISnapshotTelemetry" /> implementation.</summary>
[Immutable]
internal sealed class OpenTelemetrySnapshotTelemetry : ISnapshotTelemetry
{
    private readonly ServerHistogram2Labels _durationSeconds;

    private readonly Meter _meter;

    internal OpenTelemetrySnapshotTelemetry(Meter meter)
    {
        _meter = meter;
        _durationSeconds = new ServerHistogram2Labels(meter.CreateHistogram<double>("squirix_snapshot_duration_seconds"), "node", "result");
    }

    /// <inheritdoc />
    public ISnapshotTraceScope? BeginCreate()
    {
        var activity = ActivitySourceHolder.StartInternal("snapshot.create");
        return activity == null ? null : new Scope(activity);
    }

    /// <inheritdoc />
    public void RecordDuration(string nodeId, string result, TimeSpan elapsed) => _durationSeconds.WithLabels(nodeId, result).Observe(elapsed.TotalSeconds);

    /// <summary>Exposes how many snapshot attempts in a row have failed on this node as <c language="csharp">squirix_snapshot_consecutive_failures{node="..."}</c>.</summary>
    /// <param name="nodeId">Node identifier label.</param>
    /// <param name="consecutiveFailures">Reads the current count at each scrape.</param>
    internal void ObserveConsecutiveFailures(string nodeId, Func<int> consecutiveFailures) =>
        _ = _meter.CreateObservableGauge(
            "squirix_snapshot_consecutive_failures",
            () => new Measurement<int>(consecutiveFailures(), new KeyValuePair<string, object?>("node", nodeId)),
            "{failure}",
            "Snapshot attempts in a row that failed on this node; zero while snapshots succeed");

    [Immutable]
    private sealed class Scope : ISnapshotTraceScope
    {
        private readonly Activity _activity;

        internal Scope(Activity activity)
        {
            _activity = activity;
        }

        public void Dispose() => _activity.Dispose();

        public void SetTag(string key, string? value) => _ = _activity.SetTag(key, value);
    }
}
