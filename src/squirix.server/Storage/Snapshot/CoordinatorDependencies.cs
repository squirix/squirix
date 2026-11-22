using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Snapshot;

[Immutable]
internal sealed class CoordinatorDependencies
{
    internal CoordinatorDependencies(
        ISnapshotEntryCapture entryCapture,
        ISnapshotWriter snapWriter,
        Ledger manifestStore,
        string nodeId,
        ISnapshotTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(entryCapture);
        ArgumentNullException.ThrowIfNull(snapWriter);
        ArgumentNullException.ThrowIfNull(manifestStore);
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(telemetry);
        EntryCapture = entryCapture;
        SnapWriter = snapWriter;
        Ledger = manifestStore;
        NodeId = nodeId;
        Telemetry = telemetry;
    }

    internal ISnapshotEntryCapture EntryCapture { get; }

    internal Ledger Ledger { get; }

    internal string NodeId { get; }

    internal ISnapshotWriter SnapWriter { get; }

    internal ISnapshotTelemetry Telemetry { get; }
}
