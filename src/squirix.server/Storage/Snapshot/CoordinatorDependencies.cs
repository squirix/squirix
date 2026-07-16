using System;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class CoordinatorDependencies
{
    internal CoordinatorDependencies(
        ISnapshotEntryCapture entryCapture,
        ISnapshotWriter snapWriter,
        ManifestStore manifestStore,
        IIdempotencySnapshotExporter idempotency,
        string nodeId,
        IBackgroundSnapshotMemoryThrottle backgroundSnapshotMemoryThrottle,
        ISnapshotTelemetry? telemetry)
    {
        EntryCapture = entryCapture ?? throw new ArgumentNullException(nameof(entryCapture));
        SnapWriter = snapWriter ?? throw new ArgumentNullException(nameof(snapWriter));
        ManifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        Idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        BackgroundSnapshotMemoryThrottle = backgroundSnapshotMemoryThrottle ?? throw new ArgumentNullException(nameof(backgroundSnapshotMemoryThrottle));
        Telemetry = telemetry ?? new NoOpSnapshotTelemetry();
    }

    internal ISnapshotEntryCapture EntryCapture { get; }

    internal ISnapshotWriter SnapWriter { get; }

    internal ManifestStore ManifestStore { get; }

    internal IIdempotencySnapshotExporter Idempotency { get; }

    internal string NodeId { get; }

    internal IBackgroundSnapshotMemoryThrottle BackgroundSnapshotMemoryThrottle { get; }

    internal ISnapshotTelemetry Telemetry { get; }

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
