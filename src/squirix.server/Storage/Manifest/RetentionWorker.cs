using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Background fire-and-forget worker that schedules and runs manifest retention cleanup.</summary>
internal sealed class RetentionWorker
{
    private readonly RetentionContext _retentionContext;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private volatile State? _pendingRetentionManifest;
    private int _retentionWorkerScheduled;

    internal RetentionWorker(RetentionContext retentionContext, IRetentionCleanupReadinessStatus? retentionReadiness)
    {
        _retentionContext = retentionContext;
        _retentionReadiness = retentionReadiness;
    }

    internal void ScheduleRetentionCleanup(State manifest)
    {
        _pendingRetentionManifest = manifest;
        if (Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) is not 0)
            return;

        _ = Task.Factory.StartNew(RunRetentionWorkerLoop, CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    private void RunRetentionWorkerLoop()
    {
        try
        {
            while (_pendingRetentionManifest is { } manifest)
            {
                _pendingRetentionManifest = null;
                var cleanupFailed = RetentionCleanup.Run(_retentionContext, manifest);
                _retentionReadiness?.RecordWriteOutcome(cleanupFailed);
            }
        }
        finally
        {
            _ = Interlocked.Exchange(ref _retentionWorkerScheduled, 0);
            if (_pendingRetentionManifest is not null && Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) is 0)
            {
                _ = Task.Factory.StartNew(
                    RunRetentionWorkerLoop,
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default);
            }
        }
    }

    /// <summary>Retention cleanup for numbered manifest files, snapshots, and journal segments.</summary>
    private static class RetentionCleanup
    {
        internal static bool Run(RetentionContext context, State manifest)
        {
            var manifestCleanupFailed = TryCleanupOldManifests(context);
            var snapshotCleanupFailed = TryCleanupOldSnapshots(context, manifest.LastSnapshot);
            var journalCleanupFailed = TryCleanupObsoleteJournalSegments(context, manifest);
            return manifestCleanupFailed || snapshotCleanupFailed || journalCleanupFailed;
        }

        private static IndexedStorageFile[] GetIndexedFiles(ReadOnlySpan<string> files, Func<string, int> parseIndex)
        {
            if (files.IsEmpty)
                return [];

            var buffer = new IndexedStorageFile[files.Length];
            var writeIndex = 0;
            for (var i = 0; i < files.Length; i++)
            {
                var path = files[i];
                var index = parseIndex(Path.GetFileName(path));
                if (index <= 0)
                    continue;

                buffer[writeIndex++] = new IndexedStorageFile(path, index);
            }

            if (writeIndex is 0)
                return [];

            var result = writeIndex == buffer.Length ? buffer : Trim(buffer, writeIndex);
            Array.Sort(result, static (left, right) => right.Index.CompareTo(left.Index));
            return result;
        }

        private static void ReportRetentionCleanupException(RetentionContext context, string artifactKind, Exception exception)
        {
            context.FailureMetrics.RecordDeleteFailure(artifactKind, ManifestRetentionFailureOutcome.CleanupException);

            if (context.Logger is not null)
                LogManager.ManifestRetentionCleanupFailed(context.Logger, exception, artifactKind);
        }

        private static void ReportRetentionDeleteFailure(RetentionContext context, string artifactKind, string path)
        {
            context.FailureMetrics.RecordDeleteFailure(artifactKind, ManifestRetentionFailureOutcome.DeleteFailed);

            if (context.Logger is not null)
                LogManager.ManifestRetentionDeleteFailed(context.Logger, artifactKind, path);
        }

        private static IndexedStorageFile[] Trim(IndexedStorageFile[] buffer, int length)
        {
            var result = new IndexedStorageFile[length];
            buffer.AsSpan(0, length).CopyTo(result);
            return result;
        }

        private static bool TryCleanupObsoleteJournalSegments(RetentionContext context, State manifest)
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

        private static bool TryCleanupOldManifests(RetentionContext context)
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

        private static bool TryCleanupOldSnapshots(RetentionContext context, SnapshotRef? currentSnapshot)
        {
            try
            {
                var files = Directory.GetFiles(context.DataDir, $"{FilePrefixes.Snapshot}*{FileExtensions.Snapshot}");
                if (files.Length <= context.SnapshotRetention)
                    return false;

                var ordered = GetIndexedFiles(files, ParseSnapshotIndex);
                if (ordered.Length <= context.SnapshotRetention)
                    return false;

                return DeleteStaleSnapshots(context, ordered, BuildSnapshotKeepSet(context, ordered, currentSnapshot));
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

        private static HashSet<string> BuildSnapshotKeepSet(RetentionContext context, IndexedStorageFile[] ordered, SnapshotRef? currentSnapshot)
        {
            var keepCapacity = context.SnapshotRetention + (string.IsNullOrWhiteSpace(currentSnapshot?.Path) ? 0 : 1);
            var keep = new HashSet<string>(keepCapacity, StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < context.SnapshotRetention && i < ordered.Length; i++)
                _ = keep.Add(ordered[i].Path);

            if (!string.IsNullOrWhiteSpace(currentSnapshot?.Path))
                _ = keep.Add(currentSnapshot.Path);

            return keep;
        }

        private static bool DeleteStaleSnapshots(RetentionContext context, IndexedStorageFile[] ordered, HashSet<string> keep)
        {
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

        private static bool TryDeleteRetentionArtifact(RetentionContext context, string path, string artifactKind)
        {
            if (context.FileOperations.TryDelete(path))
                return false;

            ReportRetentionDeleteFailure(context, artifactKind, path);
            return true;
        }

        private static int ParseSnapshotIndex(ReadOnlySpan<char> name, ReadOnlySpan<char> extension)
        {
            if (name.IsEmpty)
                return 0;
            if (!name.StartsWith(FilePrefixes.Snapshot.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return 0;
            if (!name.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
                return 0;

            var numberPart = name.Slice(FilePrefixes.Snapshot.Length, name.Length - FilePrefixes.Snapshot.Length - extension.Length);
            return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
        }

        private static int ParseSnapshotIndex(string name) => ParseSnapshotIndex(name.AsSpan(), FileExtensions.Snapshot.AsSpan());

        private sealed record IndexedStorageFile(string Path, int Index);
    }
}
