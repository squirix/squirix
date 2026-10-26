using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Regression tests for snapshot manifest journal-pointer consistency.</summary>
[Immutable]
public sealed class ManifestJournalTests : IsolatedStorageTestBase
{
    /// <summary>
    /// When a roll is published during the snapshot build, the published manifest never moves
    /// the journal pointer backward to the captured segment.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AheadManifestNeverMovesBackward(CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        await store.WriteAsync(new State { CurrentJournal = 3, NextSequence = 2 }, cancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, cancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(published.CurrentJournal).IsEqualTo(3);
        _ = await Assert.That(published.LastSnapshot?.ReplayFromJournalSegment).IsEqualTo(2);
    }

    /// <summary>A snapshot manifest publish that fails after a write began fails the journal pipeline, so no later tick captures another snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedSnapshotPublishFailsPipeline(CancellationToken cancellationToken)
    {
        // A byte-range lock held through another handle fails the slot write; only Windows enforces it.
        if (!OperatingSystem.IsWindows())
            return;

        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 1);

        using (new ExclusiveRangeLock(Path.Join(Dir, "man-current"), SlotFormat.FileSize))
            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(SnapshotOnceAsync(store, journal, cancellationToken));

        _ = await Assert.That(store.PublishFailure).IsNotNull();
        _ = await Assert.That(journal.FailedWith).IsSameReferenceAs(store.PublishFailure);
    }

    /// <summary>
    /// A segment roll queued on the publisher around the snapshot manifest write keeps its journal pointer and next sequence in the
    /// manifest a restart reads. With <paramref name="rollDuringFileWrite" /> the roll is committed before the snapshot re-reads the
    /// manifest; otherwise it is queued right after that re-read, ahead of the snapshot write. The second case relies on the
    /// snapshot coordinator reading the clock for the snapshot reference after that re-read, so it stops exercising the race if that
    /// clock read moves earlier.
    /// </summary>
    /// <param name="rollDuringFileWrite">Whether the roll is committed while the snapshot file is written.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task SnapshotKeepsQueuedRollProgress(bool rollDuringFileWrite, CancellationToken cancellationToken)
    {
        var clock = new RollOnClockTimeProvider();
        var options = new PersistenceOptions { DataDir = Dir };
        RollRecorder recorder;
        using (var store = new Ledger(options, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance))
        {
            recorder = new RollRecorder(store);
            await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 5 }, cancellationToken);
            await using var journal = new SnapshotCutJournal(2, 5);
            var writerExpectations = new ISnapshotWriterCreateExpectations();
            _ = writerExpectations.Setups.AllocateIndex(Arg.Any<int>()).Callback(static publishedIndex => publishedIndex + 1);
            _ = writerExpectations.Setups
                                  .WriteAsync(
                                       Arg.Any<int>(),
                                       Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
                                       Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
                                       Arg.Any<CancellationToken>()).Callback(
                                       Bind.To<(RollOnClockTimeProvider Clock, bool RollDuringFileWrite, RollRecorder Recorder), Func<int,
                                           IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>, IReadOnlyList<PersistedIdempotencyRecord>, CancellationToken,
                                           ValueTask<string>>>(
                                           (clock, rollDuringFileWrite, recorder),
                                           static shared => (_, _, _, _) =>
                                           {
                                               if (!shared.RollDuringFileWrite)
                                               {
                                                   shared.Clock.Arm(shared.Recorder.Post);
                                                   return ValueTask.FromResult("snap-test-path");
                                               }

                                               shared.Recorder.Post();
                                               return new ValueTask<string>(shared.Recorder.Committed.Task.ContinueWith(static _ => "snap-test-path", TaskScheduler.Default));
                                           }));

            await SnapshotWithClockAsync(store, journal, writerExpectations.Instance(), clock, cancellationToken);
        }

        using var reopened = new Ledger(options, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var published = await reopened.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(recorder.Failure).IsNull();
        _ = await Assert.That(published.CurrentJournal).IsEqualTo(3);
        _ = await Assert.That(published.NextSequence).IsEqualTo(9UL);
        _ = await Assert.That(published.LastSnapshot?.ReplayFromJournalSegment).IsEqualTo(2);
        _ = await Assert.That(published.LastSnapshot?.Index).IsEqualTo(1);
    }

    /// <summary>
    /// An unreferenced snapshot file above the published index is skipped: the next snapshot takes the next free
    /// index and the orphan is deleted once that snapshot is published.
    /// </summary>
    /// <param name="publishedIndex">Index the manifest names, or 0 when it names no snapshot.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(0)]
    public async Task SnapshotSkipsOrphanIndex(int publishedIndex, CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var snapshot = publishedIndex > 0 ? new SnapshotRef { Index = publishedIndex, CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 1 } : null;
        var published = new State { CurrentJournal = 1, NextSequence = 1, LastSnapshot = snapshot };
        await store.WriteAsync(published, cancellationToken);
        var orphanPath = Path.Join(Dir, $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}");
        await File.WriteAllBytesAsync(orphanPath, [7, 7, 7], cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 1);

        await SnapshotOnceAsync(store, journal, new SnapshotWriter(Dir), cancellationToken);

        var current = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(current.LastSnapshot?.Index).IsEqualTo(3);
        _ = await Assert.That(File.Exists(Path.Join(Dir, $"{FilePrefixes.Snapshot}000003{FileExtensions.Snapshot}"))).IsTrue();

        // Retention runs in the background; the orphan below the new snapshot is deleted once it has run.
        await orphanPath.WaitUntilAsync(static path => !File.Exists(path), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>
    /// When the manifest lags behind a segment roll (roll published after the snapshot capture),
    /// the published manifest carries the captured segment instead of the stale pointer.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleManifestKeepsCapturedSegment(CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, cancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(published.CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(published.LastSnapshot?.ReplayFromJournalSegment).IsEqualTo(2);
    }

    private static async Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        var writerExpectations = new ISnapshotWriterCreateExpectations();
        _ = writerExpectations.Setups.AllocateIndex(Arg.Any<int>()).Callback(static publishedIndex => publishedIndex + 1);
        _ = writerExpectations.Setups.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult("snap-test-path"));
        await SnapshotOnceAsync(store, journal, writerExpectations.Instance(), cancellationToken);
    }

    private static Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, ISnapshotWriter writer, CancellationToken cancellationToken) =>
        SnapshotWithClockAsync(store, journal, writer, TimeProvider.System, cancellationToken);

    private static async Task SnapshotWithClockAsync(
        Ledger store,
        IJournalCoordinator journal,
        ISnapshotWriter writer,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        var opt = ServerJsonSerializer.Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var captureExpectations = new ISnapshotEntryCaptureCreateExpectations();
        _ = captureExpectations.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                               .ReturnValue(default);
        var exporterExpectations = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporterExpectations.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var deps = new CoordinatorDependencies(captureExpectations.Instance(), writer, store, exporterExpectations.Instance(), "test-node", NoOpSnapshotTelemetry.Instance);
        _ = await new Coordinator(opt, journal, deps, timeProvider).SnapshotAsync(journal, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Queues a segment roll on the manifest publisher and records how the roll ended.</summary>
    [Mutable]
    private sealed class RollRecorder
    {
        private readonly Ledger _store;

        internal RollRecorder(Ledger store)
        {
            _store = store;
        }

        internal TaskCompletionSource Committed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Exception? Failure { get; private set; }

        internal void Post() => _store.EnqueueRoll(
            3,
            9,
            () => Committed.TrySetResult(),
            ex =>
            {
                Failure = ex;
                _ = Committed.TrySetException(ex);
            });
    }

    [Mutable]
    private sealed class RollOnClockTimeProvider : TimeProvider
    {
        private Action? _onNextRead;

        public override DateTimeOffset GetUtcNow()
        {
            Interlocked.Exchange(ref _onNextRead, null)?.Invoke();
            return base.GetUtcNow();
        }

        internal void Arm(Action onNextRead) => Volatile.Write(ref _onNextRead, onNextRead);
    }
}
