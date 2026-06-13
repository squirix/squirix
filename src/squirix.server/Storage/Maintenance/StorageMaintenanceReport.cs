using System.Collections.Generic;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes the current on-disk journal/manifest/snapshot state of a Squirix data directory.
/// </summary>
internal sealed class StorageMaintenanceReport
{
    /// <summary>
    /// Gets the current journal segment index recorded in manifest.
    /// </summary>
    public int CurrentJournal { get; init; }

    /// <summary>
    /// Gets a value indicating whether the CURRENT pointer file exists.
    /// </summary>
    public bool CurrentPointerExists { get; init; }

    /// <summary>
    /// Gets the manifest file name referenced by CURRENT, when available.
    /// </summary>
    public string? CurrentPointerTarget { get; init; }

    /// <summary>
    /// Gets the inspected data directory.
    /// </summary>
    public string DataDir { get; init; } = string.Empty;

    /// <summary>
    /// Gets issues detected while inspecting storage layout.
    /// </summary>
    public IReadOnlyList<string> Issues { get; init; } = [];

    /// <summary>
    /// Gets the discovered journal segment indices on disk.
    /// </summary>
    public int[] JournalSegments { get; init; } = [];

    /// <summary>
    /// Gets the last applied sequence recorded for the last snapshot, when available.
    /// </summary>
    public ulong? LastAppliedSequence { get; init; }

    /// <summary>
    /// Gets the last snapshot index recorded in manifest, when available.
    /// </summary>
    public int? LastSnapshotIndex { get; init; }

    /// <summary>
    /// Gets the snapshot path recorded in manifest, when available.
    /// </summary>
    public string? LastSnapshotPath { get; init; }

    /// <summary>
    /// Gets a value indicating whether manifest metadata was readable through the CURRENT pointer.
    /// </summary>
    public bool ManifestReadable { get; init; }

    /// <summary>
    /// Gets the next sequence number recorded in manifest.
    /// </summary>
    public ulong NextSequence { get; init; }

    /// <summary>
    /// Gets the journal replay start segment recorded for the last snapshot, when available.
    /// </summary>
    public int? ReplayFromJournalSegment { get; init; }

    /// <summary>
    /// Gets the discovered snapshot indices on disk.
    /// </summary>
    public int[] SnapshotIndices { get; init; } = [];
}
