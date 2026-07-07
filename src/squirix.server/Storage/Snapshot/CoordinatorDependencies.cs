using System;
using Squirix.Server.Runtime.Contracts;

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
        Telemetry = telemetry ?? NoOpSnapshotTelemetry.Instance;
    }

    internal ISnapshotEntryCapture EntryCapture { get; }

    internal ISnapshotWriter SnapWriter { get; }

    internal ManifestStore ManifestStore { get; }

    internal IIdempotencySnapshotExporter Idempotency { get; }

    internal string NodeId { get; }

    internal IBackgroundSnapshotMemoryThrottle BackgroundSnapshotMemoryThrottle { get; }

    internal ISnapshotTelemetry Telemetry { get; }

    internal sealed class NoOpSnapshotTelemetry : ISnapshotTelemetry
    {
        /// <summary>Gets the shared no-op instance.</summary>
        internal static NoOpSnapshotTelemetry Instance { get; } = new();

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
