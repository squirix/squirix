using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Background fire-and-forget worker that schedules and runs manifest retention cleanup.</summary>
internal sealed class RetentionWorker : IWorkPoolItem
{
    private readonly JournalCapacityResyncSignal _capacityResync;
    private readonly RetentionContext _retentionContext;
    private readonly IRetentionCleanupReadinessStatus? _retentionReadiness;
    private volatile State? _pendingRetention;
    private int _retentionWorkerScheduled;
    private int _stopped;

    internal RetentionWorker(RetentionContext retentionContext, IRetentionCleanupReadinessStatus? retentionReadiness, JournalCapacityResyncSignal capacityResync)
    {
        ArgumentNullException.ThrowIfNull(capacityResync);
        _retentionContext = retentionContext;
        _capacityResync = capacityResync;
        _retentionReadiness = retentionReadiness;
    }

    /// <summary>Gets a value indicating whether <see cref="Stop" /> has been called.</summary>
    internal bool IsStopped => Volatile.Read(ref _stopped) != 0;

    void IWorkPoolItem.Execute()
    {
        try
        {
            while (true)
            {
                // Checked right before each pass, so a stop that lands while a pass runs prevents the next one.
                var pending = Interlocked.Exchange(ref _pendingRetention, null);
                if (pending == null || IsStopped)
                    break;

                var cleanupFailed = RetentionCleanup.Run(_retentionContext, pending, _capacityResync);
                _retentionReadiness?.RecordWriteOutcome(cleanupFailed);
            }
        }
        finally
        {
            // Release the "worker running" flag, then re-arm if ScheduleRetentionCleanup
            // queued more work while this loop still held the flag.
            _ = Interlocked.Exchange(ref _retentionWorkerScheduled, 0);
            _ = TryRestartIfPendingWorkRemains();
        }
    }

    /// <summary>Schedules a cleanup pass; a pass not yet started is replaced.</summary>
    /// <param name="manifest">The manifest whose publish has mirrored both slots, so neither slot holds an older one.</param>
    internal void ScheduleRetentionCleanup(State manifest)
    {
        if (IsStopped)
            return;

        _pendingRetention = manifest;
        if (Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) != 0)
            return;

        StartRetentionWorkerLoop();
    }

    /// <summary>
    /// Stops the worker: work scheduled from now on is ignored, and a pass that has not yet taken its work does not start. A pass already
    /// running, or one that took its work just before the stop, finishes.
    /// </summary>
    internal void Stop() => Volatile.Write(ref _stopped, 1);

    private void StartRetentionWorkerLoop() => _ = WorkPool.RunAsync(this, TaskCreationOptions.LongRunning, CancellationToken.None);

    /// <summary>Restarts the retention loop when pending work remains after the previous loop released its schedule flag.</summary>
    /// <returns><see langword="true" /> when a new retention loop was scheduled; otherwise <see langword="false" />.</returns>
    private bool TryRestartIfPendingWorkRemains()
    {
        // Another thread may publish work while the drain loop exits with the schedule flag still held.
        if (_pendingRetention == null || IsStopped)
            return false;

        if (Interlocked.CompareExchange(ref _retentionWorkerScheduled, 1, 0) != 0)
            return false;

        StartRetentionWorkerLoop();
        return true;
    }

    /// <summary>Retention cleanup for snapshots and journal segments.</summary>
    private static class RetentionCleanup
    {
        internal static bool Run(RetentionContext context, State manifest, JournalCapacityResyncSignal capacityResync)
        {
            var snapshotCleanupFailed = TryCleanupOldSnapshots(context, manifest.LastSnapshot);
            var journalCleanupFailed = TryCleanupObsoleteJournalSegments(context, manifest, out var deletedSegments, out var deletedBytes);

            // Recorded after the deletes. A pass that deleted some segments and failed on others still frees the deleted ones.
            if (deletedSegments > 0)
                capacityResync.RecordDeleted(deletedSegments, deletedBytes);

            return snapshotCleanupFailed || journalCleanupFailed;
        }

        private static void ReportRetentionCleanupException(RetentionContext context, string artifactKind, Exception exception)
        {
            context.FailureMetrics.RecordDeleteFailure(artifactKind, ManifestRetentionFailureOutcome.CleanupException);

            ServerLog.ManifestRetentionCleanupFailed(context.Logger, exception, artifactKind);
        }

        private static void ReportRetentionDeleteFailure(RetentionContext context, string artifactKind, string path)
        {
            context.FailureMetrics.RecordDeleteFailure(artifactKind, ManifestRetentionFailureOutcome.DeleteFailed);

            ServerLog.ManifestRetentionDeleteFailed(context.Logger, artifactKind, path);
        }

        private static bool TryCleanupObsoleteJournalSegments(RetentionContext context, State manifest, out int deletedSegments, out long deletedBytes)
        {
            deletedSegments = 0;
            deletedBytes = 0L;
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

                    // Read before the delete; a segment below the replay floor is closed, so its length is the bytes it counted.
                    var length = ReadLengthOrZero(segment.Path);
                    if (TryDeleteRetentionArtifact(context, segment.Path, ManifestRetentionArtifactKind.JournalSegment))
                    {
                        failed = true;
                        continue;
                    }

                    deletedSegments++;
                    deletedBytes += length;
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

        /// <summary>
        /// Deletes every snapshot below the manifest's current one; files at or above it are kept. Retention is scheduled only after a publish that
        /// mirrored both slots, so neither slot can hold a manifest whose snapshot is below the current one. A file above the current one is an
        /// in-flight write or an orphan, which a later snapshot's retention deletes. A failed delete is retried on the next run.
        /// </summary>
        /// <param name="context">The retention inputs.</param>
        /// <param name="currentSnapshot">The snapshot the manifest names, or <see langword="null" /> when it names none.</param>
        /// <returns><see langword="true" /> when a delete or the directory scan failed; otherwise <see langword="false" />.</returns>
        private static bool TryCleanupOldSnapshots(RetentionContext context, SnapshotRef? currentSnapshot)
        {
            if (currentSnapshot is not { Index: > 0 })
                return false;

            try
            {
                var dataDir = FilePathValidator.ResolveValidatedDirectoryPath(context.DataDir);
                var failed = false;
                foreach (var path in Directory.GetFiles(dataDir, $"{FilePrefixes.Snapshot}*{FileExtensions.Snapshot}"))
                {
                    var index = SnapshotFileName.ParseIndex(Path.GetFileName(path));
                    if (index <= 0 || index >= currentSnapshot.Index)
                        continue;

                    failed |= TryDeleteRetentionArtifact(context, path, ManifestRetentionArtifactKind.Snapshot);
                }

                return failed;
            }
            catch (ArgumentException ex)
            {
                ReportRetentionCleanupException(context, ManifestRetentionArtifactKind.Snapshot, ex);
                return true;
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

        private static long ReadLengthOrZero(string path)
        {
            try
            {
                return new FileInfo(path).Length;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Zero keeps the journal counters above the on-disk total.
                return 0L;
            }
        }

        private static bool TryDeleteRetentionArtifact(RetentionContext context, string path, string artifactKind)
        {
            if (context.FileOperations.TryDelete(path))
                return false;

            ReportRetentionDeleteFailure(context, artifactKind, path);
            return true;
        }
    }
}
