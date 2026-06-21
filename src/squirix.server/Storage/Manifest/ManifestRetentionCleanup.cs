using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Retention cleanup for numbered manifest files, snapshots, and journal segments.</summary>
internal static class ManifestRetentionCleanup
{
    internal static bool Run(ManifestRetentionContext context, ManifestState manifest)
    {
        var manifestCleanupFailed = TryCleanupOldManifests(context);
        var snapshotCleanupFailed = TryCleanupOldSnapshots(context, manifest.LastSnapshot);
        var journalCleanupFailed = TryCleanupObsoleteJournalSegments(context, manifest);
        return manifestCleanupFailed || snapshotCleanupFailed || journalCleanupFailed;
    }

    private static IndexedStorageFile[] GetIndexedFiles(string[] files, Func<string, int> parseIndex)
    {
        var result = new List<IndexedStorageFile>();
        foreach (var path in files)
        {
            var index = parseIndex(Path.GetFileName(path));
            if (index > 0)
                result.Add(new IndexedStorageFile(path, index));
        }

        result.Sort(static (left, right) => right.Index.CompareTo(left.Index));
        return [.. result];
    }

    private static int TryParseSnapshotIndex(string name)
    {
        if (string.IsNullOrEmpty(name))
            return 0;
        if (!name.StartsWith(StorageFilePrefixes.Snapshot, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(StorageFileExtensions.Snapshot, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Substring(StorageFilePrefixes.Snapshot.Length, name.Length - StorageFilePrefixes.Snapshot.Length - StorageFileExtensions.Snapshot.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private static void ReportRetentionCleanupException(ManifestRetentionContext context, string artifactKind, Exception exception)
    {
        StorageRetentionMetrics.DeleteFailuresTotal.WithLabels(artifactKind, ManifestRetentionFailureOutcome.CleanupException).Inc(1);

        if (context.Logger is not null)
            LogManager.ManifestRetentionCleanupFailed(context.Logger, exception, artifactKind);
    }

    private static void ReportRetentionDeleteFailure(ManifestRetentionContext context, string artifactKind, string path)
    {
        StorageRetentionMetrics.DeleteFailuresTotal.WithLabels(artifactKind, ManifestRetentionFailureOutcome.DeleteFailed).Inc(1);

        if (context.Logger is not null)
            LogManager.ManifestRetentionDeleteFailed(context.Logger, artifactKind, path);
    }

    private static bool TryCleanupObsoleteJournalSegments(ManifestRetentionContext context, ManifestState manifest)
    {
        try
        {
            var replayFromSegment = manifest.LastSnapshot?.ReplayFromJournalSegment ?? 0;
            if (replayFromSegment <= 1)
                return false;

            if (manifest.CurrentJournal < replayFromSegment)
                return false;

            var failed = false;
            foreach (var segment in JournalReader.EnumerateSegments(context.DataDir, 1))
            {
                if (segment.Index >= replayFromSegment)
                    continue;

                if (segment.Index >= manifest.CurrentJournal)
                    continue;

                failed |= TryDeleteRetentionArtifact(context, segment.Path, ManifestRetentionArtifactKind.JournalSegment);
            }

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.JournalSegment, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.JournalSegment, ex);
            return true;
        }
    }

    private static bool TryCleanupOldManifests(ManifestRetentionContext context)
    {
        try
        {
            var files = Directory.GetFiles(context.DataDir, context.ManifestFileGlob);
            if (files.Length <= context.ManifestRetention)
                return false;

            var ordered = GetIndexedFiles(files, context.ParseManifestIndex);

            if (ordered.Length <= context.ManifestRetention)
                return false;

            var failed = false;
            for (var i = context.ManifestRetention; i < ordered.Length; i++)
                failed |= TryDeleteRetentionArtifact(context, ordered[i].Path, ManifestRetentionArtifactKind.Manifest);

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.Manifest, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.Manifest, ex);
            return true;
        }
    }

    private static bool TryCleanupOldSnapshots(ManifestRetentionContext context, ManifestState.SnapshotRef? currentSnapshot)
    {
        try
        {
            var files = Directory.GetFiles(context.DataDir, $"{StorageFilePrefixes.Snapshot}*{StorageFileExtensions.Snapshot}");
            if (files.Length <= context.SnapshotRetention)
                return false;

            var ordered = GetIndexedFiles(files, TryParseSnapshotIndex);

            if (ordered.Length <= context.SnapshotRetention)
                return false;

            var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < context.SnapshotRetention && i < ordered.Length; i++)
                _ = keep.Add(ordered[i].Path);

            if (!string.IsNullOrWhiteSpace(currentSnapshot?.Path))
                _ = keep.Add(currentSnapshot.Path);

            var failed = false;
            for (var i = context.SnapshotRetention; i < ordered.Length; i++)
            {
                var stale = ordered[i];
                if (keep.Contains(stale.Path))
                    continue;

                failed |= TryDeleteRetentionArtifact(context, stale.Path, ManifestRetentionArtifactKind.Snapshot);
            }

            return failed;
        }
        catch (IOException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.Snapshot, ex);
            return true;
        }
        catch (UnauthorizedAccessException ex)
        {
            ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.Snapshot, ex);
            return true;
        }
    }

    private static bool TryDeleteRetentionArtifact(ManifestRetentionContext context, string path, string artifactKind)
    {
        if (context.FileOperations.TryDelete(path))
            return false;

        ReportRetentionDeleteFailure(context, artifactKind, path);
        return true;
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
