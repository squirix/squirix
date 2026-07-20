using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Tests that manifest retention cleanup failures are observable without breaking manifest commits.</summary>
public sealed class StoreRetentionObservabilityTests : ServerUnitTestBase
{
    private static readonly ManifestRetentionFailureMetrics RetentionFailureMetrics = ManifestRetentionFailureMetrics.Instance;

    /// <summary>Ensures repeated retention cleanup failures degrade readiness while manifest commits keep succeeding.</summary>
    [Fact]
    public async Task RepeatedRetentionFailuresDegradeReadinessWithoutBreakingWrites()
    {
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("manifest-retention-readiness");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            ManifestRetentionCount = 1,
            RetentionCleanupDegradedConsecutiveWrites = 2,
            RetentionCleanupDegradedWindowFailures = 10,
        };
        var readiness = new RetentionCleanupReadiness(options);
        var staleManifest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        await File.WriteAllBytesAsync(staleManifest, [0x53, 0x51, 0x4D, 0x46, 0x01], DefaultCancellationToken);
        using var store = new ManifestStore(options, logger, readiness, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleManifest));

        await store.WriteAsync(new State { CurrentJournal = 1 }, DefaultCancellationToken);
        await StoreTestSupport.WaitUntilAsync(readiness, static r => r.ConsecutiveWriteFailures is 1, TimeSpan.FromSeconds(5), DefaultCancellationToken);
        Assert.False(readiness.IsDegraded);
        Assert.Equal(1, readiness.ConsecutiveWriteFailures);

        await store.WriteAsync(new State { CurrentJournal = 2 }, DefaultCancellationToken);
        await StoreTestSupport.WaitUntilAsync(readiness, static r => r is { IsDegraded: true, ConsecutiveWriteFailures: 2 }, TimeSpan.FromSeconds(5), DefaultCancellationToken);
        Assert.True(readiness.IsDegraded);
        Assert.Equal(2, readiness.ConsecutiveWriteFailures);

        var stale = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        if (File.Exists(stale))
            File.SetAttributes(stale, FileAttributes.Normal);
    }

    /// <summary>Ensures a failed obsolete journal segment delete emits the journal failure metric and log while the manifest commit succeeds.</summary>
    [Fact]
    public async Task WriteSucceedsWhenJournalRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("journal-retention-delete-failure");
        var staleJournalSegment = NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000001);
        var currentJournalPath = NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000003);
        await File.WriteAllTextAsync(staleJournalSegment, "stale journal", DefaultCancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000002), "obsolete journal", DefaultCancellationToken);
        await File.WriteAllTextAsync(currentJournalPath, "current journal", DefaultCancellationToken);
        var options = new PersistenceOptions { DataDir = dir };
        using var store = new ManifestStore(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleJournalSegment));
        await store.WriteAsync(
            new State
            {
                CurrentJournal = 3,
                LastSnapshot = new SnapshotRef
                {
                    Index = 1,
                    Path = NodePathKit.Combine(dir, StoreTestSupport.Snapshot000001),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 20,
                    ReplayFromJournalSegment = 3,
                },
            },
            DefaultCancellationToken);

        await StoreTestSupport.WaitUntilAsync(
            logger,
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("journal_segment", StringComparison.OrdinalIgnoreCase)),
            TimeSpan.FromSeconds(5),
            DefaultCancellationToken);

        Assert.True(File.Exists(currentJournalPath));
        Assert.True(File.Exists(staleJournalSegment));
        Assert.Contains(logger.Entries, static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("journal_segment", StringComparison.OrdinalIgnoreCase));
        Assert.True(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.JournalSegment),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));

        RestoreNormalAttributes(staleJournalSegment);
    }

    /// <summary>Ensures a read-only obsolete manifest is retained, emits a metric, and logs a warning while the new manifest commits.</summary>
    [Fact]
    public async Task WriteSucceedsWhenManifestRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("manifest-retention-delete-failure");
        var options = new PersistenceOptions { DataDir = dir, ManifestRetentionCount = 2 };
        var staleManifest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        using var store = new ManifestStore(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleManifest));
        await store.WriteAsync(new State { CurrentJournal = 1 }, DefaultCancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2 }, DefaultCancellationToken);

        Assert.True(File.Exists(staleManifest));
        await store.WriteAsync(new State { CurrentJournal = 3 }, DefaultCancellationToken);

        await StoreTestSupport.WaitUntilAsync(
            logger,
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("manifest", StringComparison.OrdinalIgnoreCase)),
            TimeSpan.FromSeconds(5),
            DefaultCancellationToken);

        var latest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000003);
        Assert.True(File.Exists(latest));
        Assert.True(File.Exists(staleManifest));
        Assert.Contains(logger.Entries, static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("manifest", StringComparison.OrdinalIgnoreCase));
        Assert.True(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.Manifest),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));

        var stale = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        if (File.Exists(stale))
            File.SetAttributes(stale, FileAttributes.Normal);
    }

    /// <summary>Ensures a failed snapshot retention delete emits the snapshot failure metric and log while the manifest commit succeeds.</summary>
    [Fact]
    public async Task WriteSucceedsWhenSnapshotRetentionDeleteFailsAndFailureIsObservable()
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("snapshot-retention-delete-failure");
        var staleSnapshot = NodePathKit.Combine(dir, StoreTestSupport.Snapshot000001);
        var currentSnapshot = NodePathKit.Combine(dir, StoreTestSupport.Snapshot000002);
        await File.WriteAllTextAsync(staleSnapshot, "stale snapshot", DefaultCancellationToken);
        await File.WriteAllTextAsync(currentSnapshot, "current snapshot", DefaultCancellationToken);
        var options = new PersistenceOptions
        {
            DataDir = dir,
            SnapshotRetentionCount = 1,
        };
        using var store = new ManifestStore(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleSnapshot));
        await store.WriteAsync(
            new State
            {
                CurrentJournal = 2,
                LastSnapshot = new SnapshotRef
                {
                    Index = 2,
                    Path = currentSnapshot,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 20,
                    ReplayFromJournalSegment = 2,
                },
            },
            DefaultCancellationToken);

        await StoreTestSupport.WaitUntilAsync(
            logger,
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("snapshot", StringComparison.OrdinalIgnoreCase)),
            TimeSpan.FromSeconds(5),
            DefaultCancellationToken);

        Assert.True(File.Exists(currentSnapshot));
        Assert.True(File.Exists(staleSnapshot));
        Assert.Contains(logger.Entries, static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("snapshot", StringComparison.OrdinalIgnoreCase));
        Assert.True(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.Snapshot),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed)));

        RestoreNormalAttributes(staleSnapshot);
    }

    private static void RestoreNormalAttributes(string path)
    {
        if (File.Exists(path))
            File.SetAttributes(path, FileAttributes.Normal);
    }

    private sealed class CollectingLogger : ILogger<ManifestStore>
    {
        internal List<(LogLevel Level, string Message)> Entries { get; } = [];

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;

        bool ILogger.IsEnabled(LogLevel logLevel) => true;

        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    private sealed class DeleteFailingStorageFileOperations : IStorageFileOperations
    {
        private readonly FileOperations _inner = new();
        private readonly string _retainedPath;

        internal DeleteFailingStorageFileOperations(string retainedPath)
        {
            _retainedPath = retainedPath;
        }

        bool IStorageFileOperations.PublishSnapshot(string tempPath, string finalPath) => _inner.PublishSnapshot(tempPath, finalPath);

        bool IStorageFileOperations.TryDelete(string path) => !string.Equals(path, _retainedPath, StringComparison.OrdinalIgnoreCase) && _inner.TryDelete(path);
    }
}
