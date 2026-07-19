using System;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class CompletedEventArgs : EventArgs
{
    internal CompletedEventArgs(SnapshotRef snapshotRef)
    {
        SnapshotRef = snapshotRef;
    }

    internal SnapshotRef SnapshotRef { get; }
}
