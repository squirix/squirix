namespace Squirix.Server.Storage.Manifest;

internal sealed class State
{
    internal int Format { get; init; } = 1;

    internal SnapshotRef? LastSnapshot { get; init; }

    internal ulong NextSequence { get; init; } = 1;

    internal int CurrentJournal { get; init; } = 1;
}
