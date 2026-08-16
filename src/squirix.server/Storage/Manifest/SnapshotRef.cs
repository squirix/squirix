using System;
using Squirix.Attributes;

namespace Squirix.Server.Storage.Manifest;

[Immutable]
internal sealed record SnapshotRef
{
    internal DateTime CreatedUtc { get; init; }

    internal int Index { get; init; }

    internal ulong LastAppliedSequence { get; init; }

    internal string? Path { get; init; }

    internal int ReplayFromJournalSegment { get; init; } = 1;
}
