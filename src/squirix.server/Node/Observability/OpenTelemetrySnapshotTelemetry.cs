using System;
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

    internal OpenTelemetrySnapshotTelemetry(Meter meter)
    {
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
