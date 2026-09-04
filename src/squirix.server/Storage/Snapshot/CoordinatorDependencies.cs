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
        IBackgroundSnapshotMemoryThrottle backgroundSnapshotMemoryThrottle,
        ISnapshotTelemetry? telemetry)
    {
        ArgumentNullException.ThrowIfNull(entryCapture);
        ArgumentNullException.ThrowIfNull(snapWriter);
        ArgumentNullException.ThrowIfNull(manifestStore);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(backgroundSnapshotMemoryThrottle);
        EntryCapture = entryCapture;
        SnapWriter = snapWriter;
        Ledger = manifestStore;
        Idempotency = idempotency;
        NodeId = nodeId;
        BackgroundSnapshotMemoryThrottle = backgroundSnapshotMemoryThrottle;
        Telemetry = telemetry ?? new NoOpSnapshotTelemetry();
    }

    internal IBackgroundSnapshotMemoryThrottle BackgroundSnapshotMemoryThrottle { get; }

    internal ISnapshotEntryCapture EntryCapture { get; }

    internal IIdempotencySnapshotExporter Idempotency { get; }

    internal Ledger Ledger { get; }

    internal string NodeId { get; }

    internal ISnapshotWriter SnapWriter { get; }

    internal ISnapshotTelemetry Telemetry { get; }

    [Immutable]
    private sealed class NoOpSnapshotTelemetry : ISnapshotTelemetry
    {
        /// <inheritdoc />
        public ISnapshotTraceScope? BeginCreate() => null;

        /// <inheritdoc />
        public void RecordDuration(string nodeId, string result, TimeSpan elapsed)
        {
            _ = nodeId;
            _ = result;
            _ = elapsed;
        }
    }
}
