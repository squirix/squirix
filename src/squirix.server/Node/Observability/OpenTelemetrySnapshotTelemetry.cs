using System;
using System.Diagnostics;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Observability;

/// <summary>OpenTelemetry-backed <see cref="ISnapshotTelemetry" /> implementation.</summary>
internal sealed class OpenTelemetrySnapshotTelemetry : ISnapshotTelemetry
{
    /// <inheritdoc />
    public ISnapshotTraceScope? BeginCreate()
    {
        var activity = ActivitySourceHolder.StartInternal("snapshot.create");
        return activity is null ? null : new Scope(activity);
    }

    /// <inheritdoc />
    public void RecordDuration(string nodeId, string result, TimeSpan elapsed) => SnapshotMetrics.DurationSeconds.WithLabels(nodeId, result).Observe(elapsed.TotalSeconds);

    private sealed class Scope : ISnapshotTraceScope
    {
        private readonly Activity _activity;

        public Scope(Activity activity)
        {
            _activity = activity;
        }

        public void Dispose() => _activity.Dispose();

        public void SetTag(string key, object? value) => _ = _activity.SetTag(key, value);
    }
}
