using System.Text.Json.Serialization;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Manifest snapshot for admin storage diagnostics.
/// </summary>
public sealed class AdminManifestSnapshot
{
    /// <summary>Gets the current journal segment index.</summary>
    [JsonPropertyName("currentJournal")]
    public int CurrentJournal { get; init; } = 1;

    /// <summary>Gets the manifest format version.</summary>
    public int Format { get; init; } = 1;

    /// <summary>Gets the latest snapshot reference, when available.</summary>
    public AdminManifestSnapshotRef? LastSnapshot { get; init; }

    /// <summary>Gets the next journal sequence number.</summary>
    public ulong NextSequence { get; init; } = 1;
}
