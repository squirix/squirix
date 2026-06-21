using System;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class SnapshotCompletedEventArgs(Manifest.ManifestState.SnapshotRef snapshotRef) : EventArgs
{
    public Manifest.ManifestState.SnapshotRef SnapshotRef { get; } = snapshotRef;
}
