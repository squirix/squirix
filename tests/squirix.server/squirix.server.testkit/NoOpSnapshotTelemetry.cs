using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.TestKit;

/// <summary>Snapshot telemetry that traces and records nothing, for tests that do not observe snapshot telemetry.</summary>
[Immutable]
internal sealed class NoOpSnapshotTelemetry : ISnapshotTelemetry
{
    private NoOpSnapshotTelemetry()
    {
    }

    /// <summary>Gets the shared instance.</summary>
    internal static NoOpSnapshotTelemetry Instance { get; } = new();

    /// <inheritdoc />
    public ISnapshotTraceScope? BeginCreate() => null;

    /// <inheritdoc />
    public void RecordDuration(string nodeId, string result, TimeSpan elapsed)
    {
    }
}
