using System;
using System.Diagnostics;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Observability;

/// <summary>OpenTelemetry-backed <see cref="ISnapshotTelemetry" /> implementation.</summary>
[Immutable]
internal sealed class OpenTelemetrySnapshotTelemetry : ISnapshotTelemetry
{
    /// <inheritdoc />
    public ISnapshotTraceScope? BeginCreate()
    {
        var activity = ActivitySourceHolder.StartInternal("snapshot.create");
        return activity == null ? null : new Scope(activity);
    }

    /// <inheritdoc />
    public void RecordDuration(string nodeId, string result, TimeSpan elapsed) => SnapshotMetrics.DurationSeconds.WithLabels(nodeId, result).Observe(elapsed.TotalSeconds);

    private static class SnapshotMetrics
    {
        /// <summary>Labels: node, result (success|failure).</summary>
        internal static readonly ServerHistogram2Labels DurationSeconds = new(
            ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_snapshot_duration_seconds"),
            "node",
            "result");
    }

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
