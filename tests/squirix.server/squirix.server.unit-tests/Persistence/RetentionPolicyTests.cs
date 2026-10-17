using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests covering automatic retention cleanup of snapshots and journal segments.</summary>
public sealed class RetentionPolicyTests : IsolatedStorageTestBase
{
    /// <summary>Verifies journal segments older than the current snapshot replay point are removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesPruneSegmentsBeforeReplayPoint(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        CreateJournalSegment(1);
        CreateJournalSegment(2);
        CreateJournalSegment(3);
        CreateSnapshot(4);

        await store.WriteAsync(
            new State
            {
                CurrentJournal = 3,
                LastSnapshot = new SnapshotRef
                {
                    Index = 4,
                    Path = SnapshotPath(4),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 40,
                    ReplayFromJournalSegment = 3,
                },
            },
            cancellationToken);

        var staleJournalPaths = (JournalPath(1), JournalPath(2));

        // The background retention worker drains asynchronously; give the cleanup the same explicit window the burst
        // tests use under parallel CI load.
        await staleJournalPaths.WaitUntilAsync(static paths => !FileKit.Exists(paths.Item1) && !FileKit.Exists(paths.Item2), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(FileKit.Exists(JournalPath(1))).IsFalse();
        _ = await Assert.That(FileKit.Exists(JournalPath(2))).IsFalse();
        _ = await Assert.That(FileKit.Exists(JournalPath(3))).IsTrue();
    }

    /// <summary>Verifies every snapshot below the manifest's current one is deleted and the current one is kept.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesPruneSnapshotsBelowCurrent(CancellationToken cancellationToken)
    {
        using var store = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance);

        CreateSnapshot(1);
        CreateSnapshot(3);
        CreateSnapshot(4);

        await store.WriteAsync(CreateSnapshotState(4), cancellationToken);

        await WaitUntilDeletedAsync(SnapshotPath(1), cancellationToken);
        await WaitUntilDeletedAsync(SnapshotPath(3), cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(4))).IsTrue();
    }

    /// <summary>Verifies a snapshot file above the current one survives a retention run scheduled by a segment roll.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollKeepsSnapshotsAboveCurrent(CancellationToken cancellationToken)
    {
        using var store = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance);

        CreateSnapshot(1);
        CreateSnapshot(3);
        CreateSnapshot(5);
        await store.WriteAsync(CreateSnapshotState(3), cancellationToken);
        await WaitUntilDeletedAsync(SnapshotPath(1), cancellationToken);

        CreateSnapshot(2);
        store.EnqueueRoll(2, 10, static () => { }, static _ => { });
        await WaitUntilDeletedAsync(SnapshotPath(2), cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(3))).IsTrue();
        _ = await Assert.That(FileKit.Exists(SnapshotPath(5))).IsTrue();
    }

    /// <summary>Verifies a snapshot file left above the published one by a crash is kept after a restart and deleted once a later snapshot is published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartOrphanDeletedByLaterSnapshot(CancellationToken cancellationToken)
    {
        CreateSnapshot(3);
        using (var first = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance))
            await first.WriteAsync(CreateSnapshotState(3), cancellationToken);

        CreateSnapshot(4);
        using var store = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance);
        _ = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        CreateSnapshot(2);
        store.EnqueueRoll(2, 10, static () => { }, static _ => { });
        await WaitUntilDeletedAsync(SnapshotPath(2), cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(4))).IsTrue();

        CreateSnapshot(5);
        await store.WriteSnapshotAsync(CreateSnapshotState(5), cancellationToken);
        await WaitUntilDeletedAsync(SnapshotPath(4), cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(5))).IsTrue();
    }

    /// <summary>Verifies a manifest that names no snapshot, as after journal compaction, makes retention delete no snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ManifestWithoutSnapshotKeepsAllSnapshots(CancellationToken cancellationToken)
    {
        var passes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(passes.Enqueue);
        using var store = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance, readiness.Instance());

        CreateSnapshot(1);
        CreateSnapshot(2);

        await store.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);
        await passes.WaitUntilAsync(static queue => !queue.IsEmpty, cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(1))).IsTrue();
        _ = await Assert.That(FileKit.Exists(SnapshotPath(2))).IsTrue();
        _ = await Assert.That(passes).DoesNotContain(true);
    }

    /// <summary>Verifies a snapshot whose delete failed is deleted by the next retention run.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedSnapshotDeleteRetriedOnNextRun(CancellationToken cancellationToken)
    {
        var passes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(passes.Enqueue);
        var fileOperations = new FailFirstDeleteFileOperations(SnapshotPath(1));
        using var store = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance, readiness.Instance(), null, fileOperations);

        CreateSnapshot(1);
        CreateSnapshot(2);
        await store.WriteAsync(CreateSnapshotState(2), cancellationToken);
        await passes.WaitUntilAsync(static queue => !queue.IsEmpty, cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(1))).IsTrue();
        _ = await Assert.That(passes).Contains(true);

        store.EnqueueRoll(2, 10, static () => { }, static _ => { });
        await WaitUntilDeletedAsync(SnapshotPath(1), cancellationToken);

        _ = await Assert.That(FileKit.Exists(SnapshotPath(2))).IsTrue();
    }

    private static Task WaitUntilDeletedAsync(string path, CancellationToken cancellationToken) =>
        path.WaitUntilAsync(static value => !FileKit.Exists(value), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

    private State CreateSnapshotState(int index) => new()
    {
        CurrentJournal = 1,
        LastSnapshot = new SnapshotRef
        {
            Index = index,
            Path = SnapshotPath(index),
            CreatedUtc = DateTime.UtcNow,
            LastAppliedSequence = 10,
            ReplayFromJournalSegment = 1,
        },
    };

    private void CreateJournalSegment(int index) => FileKit.WriteAllText(JournalPath(index), $"journal-{NodeInvariantIndexStrings.Format(index)}");

    private void CreateSnapshot(int index) => FileKit.WriteAllText(SnapshotPath(index), $"snapshot-{NodeInvariantIndexStrings.Format(index)}");

    private string JournalPath(int index) => NodePathKit.Combine(false, Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");

    private string SnapshotPath(int index) => NodePathKit.Combine(false, Dir, $"{FilePrefixes.Snapshot}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Snapshot}");

    /// <summary>File operations whose first delete of one path fails and whose later deletes succeed.</summary>
    private sealed class FailFirstDeleteFileOperations : IStorageFileOperations
    {
        private readonly FileOperations _inner = new();
        private readonly string _path;
        private int _failed;

        internal FailFirstDeleteFileOperations(string path)
        {
            _path = path;
        }

        bool IStorageFileOperations.PublishSnapshot(string tempPath, string finalPath) => _inner.PublishSnapshot(tempPath, finalPath);

        bool IStorageFileOperations.TryDelete(string path) =>
            !(string.Equals(path, _path, StringComparison.OrdinalIgnoreCase) && Interlocked.Exchange(ref _failed, 1) == 0) && _inner.TryDelete(path);
    }
}
