using System;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class SnapshotCompletedEventArgs(Manifest.SnapshotRef snapshotRef) : EventArgs
{
    public Manifest.SnapshotRef SnapshotRef { get; } = snapshotRef;
}
