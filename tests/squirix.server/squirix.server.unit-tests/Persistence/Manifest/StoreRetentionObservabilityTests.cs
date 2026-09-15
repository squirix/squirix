using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Tests that manifest retention cleanup failures are observable without breaking manifest commits.</summary>
[Immutable]
public sealed class StoreRetentionObservabilityTests : ServerUnitTestBase
{
    private static readonly Meter RetentionFailureMeter = new("Squirix");
    private static readonly ManifestRetentionFailureMetrics RetentionFailureMetrics = new(RetentionFailureMeter);
    private static readonly byte[] StaleManifestBytes = [0x53, 0x51, 0x4D, 0x46, 0x01];

    /// <summary>Ensures a failed obsolete journal segment delete emits the journal failure metric and log while the manifest commit succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalFailureObservableOnWrite(CancellationToken cancellationToken)
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("journal-retention-delete-failure");
        var staleJournalSegment = NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000001);
        var currentJournalPath = NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000003);
        await File.WriteAllTextAsync(staleJournalSegment, "stale journal", cancellationToken);
        await File.WriteAllTextAsync(NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000002), "obsolete journal", cancellationToken);
        await File.WriteAllTextAsync(currentJournalPath, "current journal", cancellationToken);
        var options = new PersistenceOptions { DataDir = dir };
        using var store = new Ledger(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleJournalSegment));
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
            cancellationToken);

        await logger.WaitUntilAsync(
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("journal_segment", StringComparison.OrdinalIgnoreCase)),
            cancellationToken);

        _ = await Assert.That(File.Exists(currentJournalPath)).IsTrue();
        _ = await Assert.That(File.Exists(staleJournalSegment)).IsTrue();
        _ = await Assert.That(logger.Entries)
                        .Contains(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("journal_segment", StringComparison.OrdinalIgnoreCase));
        _ = await Assert.That(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.JournalSegment),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed))).IsTrue();

        RestoreNormalAttributes(staleJournalSegment);
    }

    /// <summary>Ensures a read-only obsolete manifest is retained, emits a metric, and logs a warning while the new manifest commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ManifestFailureObservableOnWrite(CancellationToken cancellationToken)
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("manifest-retention-delete-failure");
        var options = new PersistenceOptions { DataDir = dir, ManifestRetentionCount = 2 };
        var staleManifest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        using var store = new Ledger(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleManifest));
        await store.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2 }, cancellationToken);

        _ = await Assert.That(File.Exists(staleManifest)).IsTrue();
        await store.WriteAsync(new State { CurrentJournal = 3 }, cancellationToken);

        await logger.WaitUntilAsync(
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("manifest", StringComparison.OrdinalIgnoreCase)),
            cancellationToken);

        var latest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000003);
        _ = await Assert.That(File.Exists(latest)).IsTrue();
        _ = await Assert.That(File.Exists(staleManifest)).IsTrue();
        _ = await Assert.That(logger.Entries).Contains(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("manifest", StringComparison.OrdinalIgnoreCase));
        _ = await Assert.That(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.Manifest),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed))).IsTrue();

        var stale = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        if (File.Exists(stale))
            File.SetAttributes(stale, FileAttributes.Normal);
    }

    /// <summary>Ensures repeated retention cleanup failures degrade readiness while manifest commits keep succeeding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedRetentionFailuresDegradeReady(CancellationToken cancellationToken)
    {
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("manifest-retention-readiness");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            ManifestRetentionCount = 1,
            RetentionCleanupDegradedWrites = 2,
            RetentionCleanupDegradedWindowFailures = 10,
        };
        var readiness = new RetentionCleanupReadiness(options);
        var staleManifest = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        await File.WriteAllBytesAsync(staleManifest, StaleManifestBytes, cancellationToken);
        using var store = new Ledger(options, logger, readiness, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleManifest));

        await store.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);
        await readiness.WaitUntilAsync(static r => r.ConsecutiveWriteFailures == 1, cancellationToken);
        _ = await Assert.That(readiness.IsDegraded).IsFalse();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(1);

        await store.WriteAsync(new State { CurrentJournal = 2 }, cancellationToken);
        await readiness.WaitUntilAsync(static r => r is { IsDegraded: true, ConsecutiveWriteFailures: 2 }, cancellationToken);
        _ = await Assert.That(readiness.IsDegraded).IsTrue();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(2);

        var stale = NodePathKit.Combine(dir, StoreTestSupport.Manifest000001);
        if (File.Exists(stale))
            File.SetAttributes(stale, FileAttributes.Normal);
    }

    /// <summary>Ensures a failed snapshot retention delete emits the snapshot failure metric and log while the manifest commit succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotFailureObservableOnWrite(CancellationToken cancellationToken)
    {
        using var sink = new NodeMeasurementSink("Squirix");
        var logger = new CollectingLogger();
        using var dir = new TempDirectory("snapshot-retention-delete-failure");
        var staleSnapshot = NodePathKit.Combine(dir, StoreTestSupport.Snapshot000001);
        var currentSnapshot = NodePathKit.Combine(dir, StoreTestSupport.Snapshot000002);
        await File.WriteAllTextAsync(staleSnapshot, "stale snapshot", cancellationToken);
        await File.WriteAllTextAsync(currentSnapshot, "current snapshot", cancellationToken);
        var options = new PersistenceOptions
        {
            DataDir = dir,
            SnapshotRetentionCount = 1,
        };
        using var store = new Ledger(options, logger, null, RetentionFailureMetrics, new DeleteFailingStorageFileOperations(staleSnapshot));
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
            cancellationToken);

        await logger.WaitUntilAsync(
            static log => log.Entries.Exists(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("snapshot", StringComparison.OrdinalIgnoreCase)),
            cancellationToken);

        _ = await Assert.That(File.Exists(currentSnapshot)).IsTrue();
        _ = await Assert.That(File.Exists(staleSnapshot)).IsTrue();
        _ = await Assert.That(logger.Entries).Contains(static entry => entry.Level is LogLevel.Warning && entry.Message.Contains("snapshot", StringComparison.OrdinalIgnoreCase));
        _ = await Assert.That(
            sink.HasEvent(
                "squirix_storage_retention_delete_failures_total",
                ("artifact", ManifestRetentionArtifactKind.Snapshot),
                ("outcome", ManifestRetentionFailureOutcome.DeleteFailed))).IsTrue();

        RestoreNormalAttributes(staleSnapshot);
    }

    private static void RestoreNormalAttributes(string path)
    {
        if (File.Exists(path))
            File.SetAttributes(path, FileAttributes.Normal);
    }

    [Immutable]
    private sealed class CollectingLogger : ILogger<Ledger>
    {
        internal List<(LogLevel Level, string Message)> Entries { get; } = [];

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;

        bool ILogger.IsEnabled(LogLevel logLevel) => true;

        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    [Immutable]
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
