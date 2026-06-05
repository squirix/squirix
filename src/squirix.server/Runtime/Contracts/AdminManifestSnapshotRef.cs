using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Snapshot reference embedded in admin storage manifest diagnostics.
/// </summary>
public sealed class AdminManifestSnapshotRef
{
    /// <summary>Gets the snapshot creation timestamp.</summary>
    public DateTime CreatedUtc { get; init; }

    /// <summary>Gets the snapshot index.</summary>
    public int Index { get; init; }

    /// <summary>Gets the last journal sequence applied to the snapshot.</summary>
    public ulong LastAppliedSequence { get; init; }

    /// <summary>Gets the snapshot file path.</summary>
    public string? Path { get; init; }

    /// <summary>Gets the first journal segment required for replay.</summary>
    [JsonPropertyName("replayFromJournalSegment")]
    public int ReplayFromJournalSegment { get; init; } = 1;
}
