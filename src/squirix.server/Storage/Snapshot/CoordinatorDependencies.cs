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
        IIdempotencySnapshotExporter idempotency,
        string nodeId,
        ISnapshotTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(entryCapture);
        ArgumentNullException.ThrowIfNull(snapWriter);
        ArgumentNullException.ThrowIfNull(manifestStore);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(telemetry);
        EntryCapture = entryCapture;
        SnapWriter = snapWriter;
        Ledger = manifestStore;
        Idempotency = idempotency;
        NodeId = nodeId;
        Telemetry = telemetry;
    }

    internal ISnapshotEntryCapture EntryCapture { get; }

    internal IIdempotencySnapshotExporter Idempotency { get; }

    internal Ledger Ledger { get; }

    internal string NodeId { get; }

    internal ISnapshotWriter SnapWriter { get; }

    internal ISnapshotTelemetry Telemetry { get; }
}
