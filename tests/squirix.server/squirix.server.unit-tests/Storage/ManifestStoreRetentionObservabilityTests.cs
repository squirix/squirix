using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Tests that manifest retention cleanup failures are observable without breaking manifest commits.
/// </summary>
public sealed class ManifestStoreRetentionObservabilityTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures a read-only obsolete manifest is retained, emits a metric, and logs a warning while the new manifest commits.
    /// </summary>
    [Fact]
    public void WriteSucceedsWhenManifestRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new MeasurementSink("Squirix");
        var logger = new CollectingLogger();
        var dir = DirectoryKit.CreateTempDirectory("manifest-retention-delete-failure");
        try
        {
            var options = new PersistenceOptions { DataDir = dir, ManifestRetentionCount = 2 };
            var staleManifest = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
            var store = new ManifestStore(options, logger, new DeleteFailingStorageFileOperations(staleManifest));
            store.Write(new Manifest { CurrentJournal = 1 });
            store.Write(new Manifest { CurrentJournal = 2 });

            Assert.True(File.Exists(staleManifest));
            store.Write(new Manifest { CurrentJournal = 3 });

            var latest = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000003{StorageFileExtensions.Manifest}");
            Assert.True(File.Exists(latest));
            Assert.True(File.Exists(staleManifest));
            Assert.Contains(logger.Entries, static entry => entry.Level == LogLevel.Warning && entry.Message.Contains("manifest", StringComparison.OrdinalIgnoreCase));
            Assert.True(
                sink.HasEvent(
                    "squirix_storage_retention_delete_failures_total",
                    ("artifact", ManifestRetentionArtifactKind.Manifest),
                    ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));
        }
        finally
        {
            var stale = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
            if (File.Exists(stale))
            {
                File.SetAttributes(stale, FileAttributes.Normal);
            }

            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures a failed snapshot retention delete emits the snapshot failure metric and log while the manifest commit succeeds.
    /// </summary>
    [Fact]
    public void WriteSucceedsWhenSnapshotRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new MeasurementSink("Squirix");
        var logger = new CollectingLogger();
        var dir = DirectoryKit.CreateTempDirectory("snapshot-retention-delete-failure");
        var staleSnapshot = PathKit.Combine(dir, $"{StorageFilePrefixes.Snapshot}000001{StorageFileExtensions.Snapshot}");
        try
        {
            var currentSnapshot = PathKit.Combine(dir, $"{StorageFilePrefixes.Snapshot}000002{StorageFileExtensions.Snapshot}");
            File.WriteAllText(staleSnapshot, "stale snapshot");
            File.WriteAllText(currentSnapshot, "current snapshot");
            var options = new PersistenceOptions
            {
                DataDir = dir,
                SnapshotRetentionCount = 1,
            };
            var store = new ManifestStore(options, logger, new DeleteFailingStorageFileOperations(staleSnapshot));
            store.Write(
                new Manifest
                {
                    CurrentJournal = 2,
                    LastSnapshot = new Manifest.SnapshotRef
                    {
                        Index = 2,
                        Path = currentSnapshot,
                        CreatedUtc = DateTime.UtcNow,
                        LastAppliedSequence = 20,
                        ReplayFromJournalSegment = 2,
                    },
                });

            Assert.True(File.Exists(currentSnapshot));
            Assert.True(File.Exists(staleSnapshot));
            Assert.Contains(logger.Entries, static entry => entry.Level == LogLevel.Warning && entry.Message.Contains("snapshot", StringComparison.OrdinalIgnoreCase));
            Assert.True(
                sink.HasEvent(
                    "squirix_storage_retention_delete_failures_total",
                    ("artifact", ManifestRetentionArtifactKind.Snapshot),
                    ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));
        }
        finally
        {
            RestoreNormalAttributes(staleSnapshot);
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures a failed obsolete journal segment delete emits the journal failure metric and log while the manifest commit succeeds.
    /// </summary>
    [Fact]
    public void WriteSucceedsWhenJournalRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new MeasurementSink("Squirix");
        var logger = new CollectingLogger();
        var dir = DirectoryKit.CreateTempDirectory("journal-retention-delete-failure");
        var staleJournalSegment = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        try
        {
            var currentJournalPath = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000003{StorageFileExtensions.Journal}");
            File.WriteAllText(staleJournalSegment, "stale journal");
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000002{StorageFileExtensions.Journal}"), "obsolete journal");
            File.WriteAllText(currentJournalPath, "current journal");
            var options = new PersistenceOptions { DataDir = dir };
            var store = new ManifestStore(options, logger, new DeleteFailingStorageFileOperations(staleJournalSegment));
            store.Write(
                new Manifest
                {
                    CurrentJournal = 3,
                    LastSnapshot = new Manifest.SnapshotRef
                    {
                        Index = 1,
                        Path = PathKit.Combine(dir, $"{StorageFilePrefixes.Snapshot}000001{StorageFileExtensions.Snapshot}"),
                        CreatedUtc = DateTime.UtcNow,
                        LastAppliedSequence = 20,
                        ReplayFromJournalSegment = 3,
                    },
                });

            Assert.True(File.Exists(currentJournalPath));
            Assert.True(File.Exists(staleJournalSegment));
            Assert.Contains(logger.Entries, static entry => entry.Level == LogLevel.Warning && entry.Message.Contains("journal_segment", StringComparison.OrdinalIgnoreCase));
            Assert.True(
                sink.HasEvent(
                    "squirix_storage_retention_delete_failures_total",
                    ("artifact", ManifestRetentionArtifactKind.JournalSegment),
                    ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));
        }
        finally
        {
            RestoreNormalAttributes(staleJournalSegment);
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static void RestoreNormalAttributes(string path)
    {
        if (File.Exists(path))
            File.SetAttributes(path, FileAttributes.Normal);
    }

    private sealed class CollectingLogger : ILogger<ManifestStore>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    private sealed class DeleteFailingStorageFileOperations(string retainedPath) : IStorageFileOperations
    {
        private readonly StorageFileOperations _inner = new();

        public void PublishSnapshot(string tempPath, string finalPath) => _inner.PublishSnapshot(tempPath, finalPath);

        public bool TryDelete(string path) =>
            !string.Equals(path, retainedPath, StringComparison.OrdinalIgnoreCase) && _inner.TryDelete(path);
    }
}
