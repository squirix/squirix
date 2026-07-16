using System;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class CompletedEventArgs(State.SnapshotRef snapshotRef) : EventArgs
{
    internal State.SnapshotRef SnapshotRef { get; } = snapshotRef;
}
