using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Provides offline inspection, compaction, and repair operations for journal/manifest storage.
/// </summary>
internal static class StorageMaintenanceTool
{
    /// <summary>
    /// Inspects the specified data directory and returns a report describing manifest, snapshot, and journal state.
    /// </summary>
    /// <param name="dataDir">Storage data directory to inspect.</param>
    /// <returns>A report describing the current on-disk state.</returns>
    public static StorageMaintenanceReport Inspect(string dataDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);

        var issues = new List<string>();
        var snapshotFiles = Directory.Exists(dataDir)
            ? Directory.GetFiles(dataDir, $"{StorageFilePrefixes.Snapshot}*{StorageFileExtensions.Snapshot}", SearchOption.TopDirectoryOnly)
            : [];
        var snapshotIndices = GetIndexedSnapshotFiles(snapshotFiles, false);

        var journalSegments = CollectJournalSegments(dataDir);
        var journalSegmentIndices = GetJournalSegmentIndices(journalSegments);

        var currentPointerPath = PathEx.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        var currentPointerExists = File.Exists(currentPointerPath);
        string? currentPointerTarget = null;
        if (currentPointerExists)
        {
            try
            {
                currentPointerTarget = File.ReadAllText(currentPointerPath).Trim();
                if (string.IsNullOrWhiteSpace(currentPointerTarget))
                    issues.Add("CURRENT pointer is empty.");
            }
            catch (IOException ex)
            {
                issues.Add($"Failed to read CURRENT pointer: {ex.Message}");
            }
            catch (UnauthorizedAccessException ex)
            {
                issues.Add($"Failed to read CURRENT pointer: {ex.Message}");
            }
            catch (SecurityException ex)
            {
                issues.Add($"Failed to read CURRENT pointer: {ex.Message}");
            }
            catch (NotSupportedException ex)
            {
                issues.Add($"Failed to read CURRENT pointer: {ex.Message}");
            }
        }
        else
        {
            issues.Add("CURRENT pointer is missing.");
        }

        var manifestStore = new ManifestStore(new PersistenceOptions { DataDir = dataDir });
        Manifest manifest;
        try
        {
            manifest = manifestStore.ReadCurrentOrDefault();
        }
        catch (IOException ex)
        {
            manifest = new Manifest();
            issues.Add($"Failed to read manifest: {ex.Message}");
        }
        catch (UnauthorizedAccessException ex)
        {
            manifest = new Manifest();
            issues.Add($"Failed to read manifest: {ex.Message}");
        }
        catch (InvalidDataException ex)
        {
            manifest = new Manifest();
            issues.Add($"Failed to read manifest: {ex.Message}");
        }
        catch (JsonException ex)
        {
            manifest = new Manifest();
            issues.Add($"Failed to read manifest: {ex.Message}");
        }

        var manifestReadable = currentPointerExists && !string.IsNullOrWhiteSpace(currentPointerTarget);
        if (!manifestReadable && journalSegmentIndices.Length == 0 && snapshotIndices.Length == 0)
            issues.Add("No journal segments or snapshots were found.");

        if (!string.IsNullOrWhiteSpace(currentPointerTarget))
        {
            var currentManifestPath = PathEx.Combine(dataDir, currentPointerTarget);
            if (!File.Exists(currentManifestPath))
                issues.Add($"CURRENT pointer target is missing: {currentPointerTarget}");
        }

        if (manifest.LastSnapshot is not null && !string.IsNullOrWhiteSpace(manifest.LastSnapshot.Path) && !File.Exists(manifest.LastSnapshot.Path))
            issues.Add($"Manifest snapshot path is missing: {manifest.LastSnapshot.Path}");

        if (journalSegmentIndices.Length <= 1)
            return BuildReport();

        for (var i = 1; i < journalSegmentIndices.Length; i++)
        {
            if (journalSegmentIndices[i] == journalSegmentIndices[i - 1] + 1)
                continue;

            issues.Add($"journal segments are discontinuous between {journalSegmentIndices[i - 1]} and {journalSegmentIndices[i]}.");
            break;
        }

        return BuildReport();

        StorageMaintenanceReport BuildReport()
        {
            return new StorageMaintenanceReport
            {
                DataDir = dataDir,
                ManifestReadable = manifestReadable,
                CurrentPointerExists = currentPointerExists,
                CurrentPointerTarget = currentPointerTarget,
                CurrentJournal = manifest.CurrentJournal,
                NextSequence = manifest.NextSequence,
                SnapshotIndices = GetSnapshotIndices(snapshotIndices),
                JournalSegments = journalSegmentIndices,
                LastSnapshotIndex = manifest.LastSnapshot?.Index,
                LastSnapshotPath = manifest.LastSnapshot?.Path,
                LastAppliedSequence = manifest.LastSnapshot?.LastAppliedSequence,
                ReplayFromJournalSegment = manifest.LastSnapshot?.ReplayFromJournalSegment,
                Issues = issues,
            };
        }
    }

    /// <summary>
    /// Compacts journal segments for the specified data directory using the current manifest snapshot boundary.
    /// </summary>
    /// <param name="dataDir">Storage data directory to compact.</param>
    /// <param name="strictFsync">Whether strict fsync semantics should be used while persisting manifest updates.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A result describing the post-compaction storage state.</returns>
    [UsedImplicitly]
    public static async Task<StorageMaintenanceResult> CompactAsync(string dataDir, bool strictFsync = true, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        cancellationToken.ThrowIfCancellationRequested();

        var persistence = new PersistenceOptions
        {
            DataDir = dataDir,
        };

        var manifestStore = new ManifestStore(persistence);
        await JournalCompactor.CompactAsync(persistence, manifestStore, cancellationToken).ConfigureAwait(false);

        return new StorageMaintenanceResult
        {
            Action = "compact",
            Report = Inspect(dataDir),
        };
    }

    /// <summary>
    /// Repairs manifest/CURRENT metadata conservatively so the node can recover offline state safely.
    /// </summary>
    /// <param name="dataDir">Storage data directory to repair.</param>
    /// <param name="strictFsync">Whether strict fsync semantics should be used while persisting the repaired manifest.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="ValueTask{TResult}" /> that completes with a result describing the repaired storage state.</returns>
    public static ValueTask<StorageMaintenanceResult> RepairAsync(string dataDir, bool strictFsync = true, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        cancellationToken.ThrowIfCancellationRequested();

        var persistence = new PersistenceOptions
        {
            DataDir = dataDir,
        };
        var manifestStore = new ManifestStore(persistence);
        var existing = Inspect(dataDir);
        var journalSegments = CollectJournalSegments(dataDir);
        var journalSegmentIndices = GetJournalSegmentIndices(journalSegments);
        var latestJournalSegment = journalSegmentIndices.Length == 0 ? 1 : GetMax(journalSegmentIndices);
        var nextSequence = GetNextSequence(dataDir, journalSegmentIndices);

        var repairedManifest = BuildRepairedManifest(dataDir, existing, latestJournalSegment, nextSequence);
        manifestStore.Write(repairedManifest);

        return ValueTask.FromResult(
            new StorageMaintenanceResult
            {
                Action = "repair",
                Report = Inspect(dataDir),
            });
    }

    private static Manifest BuildRepairedManifest(string dataDir, StorageMaintenanceReport existing, int latestJournalSegment, ulong nextSequence)
    {
        if (existing is { ManifestReadable: true, CurrentPointerExists: true })
        {
            return new Manifest
            {
                CurrentJournal = latestJournalSegment,
                NextSequence = nextSequence,
                LastSnapshot = existing.LastSnapshotIndex is null || string.IsNullOrWhiteSpace(existing.LastSnapshotPath)
                    ? null
                    : new Manifest.SnapshotRef
                    {
                        Index = existing.LastSnapshotIndex.Value,
                        Path = existing.LastSnapshotPath,
                        CreatedUtc = File.Exists(existing.LastSnapshotPath) ? File.GetCreationTimeUtc(existing.LastSnapshotPath) : DateTime.UtcNow,
                        LastAppliedSequence = existing.LastAppliedSequence.GetValueOrDefault(),
                        ReplayFromJournalSegment = existing.ReplayFromJournalSegment.GetValueOrDefault(1),
                    },
            };
        }

        var snapshots = Directory.Exists(dataDir)
            ? Directory.GetFiles(dataDir, $"{StorageFilePrefixes.Snapshot}*{StorageFileExtensions.Snapshot}", SearchOption.TopDirectoryOnly)
            : [];
        var hasLatestSnapshot = TryGetLatestSnapshotFile(snapshots, out var latestSnapshot);

        if (latestJournalSegment > 1 || !hasLatestSnapshot)
        {
            return new Manifest
            {
                CurrentJournal = latestJournalSegment,
                NextSequence = nextSequence,
                LastSnapshot = null,
            };
        }

        var snapshotPath = latestSnapshot.Path;

        return new Manifest
        {
            CurrentJournal = 1,
            NextSequence = 1,
            LastSnapshot = new Manifest.SnapshotRef
            {
                Index = latestSnapshot.Index,
                Path = snapshotPath,
                CreatedUtc = File.GetCreationTimeUtc(snapshotPath),
                LastAppliedSequence = 0,
                ReplayFromJournalSegment = 1,
            },
        };
    }

    private static JournalSegment[] CollectJournalSegments(string dataDir)
    {
        var result = new List<JournalSegment>();
        foreach (var segment in JournalReader.EnumerateSegments(dataDir, 1))
            result.Add(segment);

        return [.. result];
    }

    private static IndexedStorageFile[] GetIndexedSnapshotFiles(string[] snapshotFiles, bool descending)
    {
        var indexed = new List<IndexedStorageFile>();
        foreach (var path in snapshotFiles)
        {
            var index = TryParseIndexedFile(Path.GetFileName(path), StorageFilePrefixes.Snapshot, StorageFileExtensions.Snapshot);
            if (index > 0)
                indexed.Add(new IndexedStorageFile(path, index));
        }

        indexed.Sort(descending ? static (left, right) => right.Index.CompareTo(left.Index) : static (left, right) => left.Index.CompareTo(right.Index));
        return [.. indexed];
    }

    private static int[] GetJournalSegmentIndices(JournalSegment[] journalSegments)
    {
        var indices = new int[journalSegments.Length];
        for (var i = 0; i < journalSegments.Length; i++)
            indices[i] = journalSegments[i].Index;

        return indices;
    }

    private static int GetMax(int[] values)
    {
        var max = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            if (values[i] > max)
                max = values[i];
        }

        return max;
    }

    private static int GetMin(int[] values)
    {
        var min = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            if (values[i] < min)
                min = values[i];
        }

        return min;
    }

    private static ulong GetNextSequence(string dataDir, int[] journalSegmentIndices)
    {
        if (journalSegmentIndices.Length == 0)
            return 1;

        var maxSeq = 0UL;
        foreach (var env in JournalReader.ReadAll(dataDir, GetMin(journalSegmentIndices), CancellationToken.None))
        {
            if (env.Seq > maxSeq)
                maxSeq = env.Seq;
        }

        return maxSeq == 0 ? 1 : maxSeq + 1;
    }

    private static int[] GetSnapshotIndices(IndexedStorageFile[] snapshotFiles)
    {
        var indices = new int[snapshotFiles.Length];
        for (var i = 0; i < snapshotFiles.Length; i++)
            indices[i] = snapshotFiles[i].Index;

        return indices;
    }

    private static bool TryGetLatestSnapshotFile(string[] snapshotFiles, out IndexedStorageFile snapshotFile)
    {
        var indexed = GetIndexedSnapshotFiles(snapshotFiles, true);
        if (indexed.Length == 0)
        {
            snapshotFile = default;
            return false;
        }

        snapshotFile = indexed[0];
        return true;
    }

    private static int TryParseIndexedFile(string? fileName, string prefix, string extension)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return 0;
        if (!fileName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!fileName.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = fileName.Substring(prefix.Length, fileName.Length - prefix.Length - extension.Length);
        return int.TryParse(numberPart, NumberStyles.None, CultureInfo.InvariantCulture, out var idx) ? idx : 0;
    }

    private readonly struct IndexedStorageFile
    {
        public IndexedStorageFile(string path, int index)
        {
            Path = path;
            Index = index;
        }

        public int Index { get; }

        public string Path { get; }
    }
}
