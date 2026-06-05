using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage;

internal sealed class Manifest
{
    [JsonPropertyName("currentJournal")]
    public int CurrentJournal { get; init; } = 1;

    public int Format { get; init; } = 1;

    public SnapshotRef? LastSnapshot { get; init; }

    public ulong NextSequence { get; init; } = 1;

    internal sealed record SnapshotRef
    {
        public DateTime CreatedUtc { get; init; }

        public int Index { get; init; }

        public ulong LastAppliedSequence { get; init; }

        public string? Path { get; init; }

        [JsonPropertyName("replayFromJournalSegment")]
        public int ReplayFromJournalSegment { get; init; } = 1;
    }
}
