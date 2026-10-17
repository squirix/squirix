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
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance);
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 3, NextSequence = 2 }, cancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, cancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(published.CurrentJournal).IsEqualTo(3);
        _ = await Assert.That(published.LastSnapshot?.ReplayFromJournalSegment).IsEqualTo(2);
    }

    /// <summary>
    /// When the manifest lags behind a segment roll (roll published after the snapshot capture),
    /// the published manifest carries the captured segment instead of the stale pointer.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleManifestKeepsCapturedSegment(CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance);
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, cancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(published.CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(published.LastSnapshot?.ReplayFromJournalSegment).IsEqualTo(2);
    }

    /// <summary>
    /// An unreferenced snapshot file above the published index is skipped: the next snapshot takes the next free
    /// index and the orphan is left untouched.
    /// </summary>
    /// <param name="publishedIndex">Index the manifest names, or 0 when it names no snapshot.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(0)]
    public async Task SnapshotSkipsOrphanIndex(int publishedIndex, CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance);
        var snapshot = publishedIndex > 0
            ? new SnapshotRef { Index = publishedIndex, Path = "published", CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 1 }
            : null;
        var published = new State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = snapshot };
        await store.WriteAsync(published, cancellationToken);
        var orphanPath = Path.Join(Dir, $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}");
        await File.WriteAllBytesAsync(orphanPath, [7, 7, 7], cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 1);

        await SnapshotOnceAsync(store, journal, new SnapshotWriter(Dir), cancellationToken);

        var current = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(current.LastSnapshot?.Index).IsEqualTo(3);
        _ = await Assert.That(File.Exists(Path.Join(Dir, $"{FilePrefixes.Snapshot}000003{FileExtensions.Snapshot}"))).IsTrue();
        var orphanBytes = await File.ReadAllBytesAsync(orphanPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([7, 7, 7], orphanBytes);
    }

    private static async Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        var writerExpectations = new ISnapshotWriterCreateExpectations();
        _ = writerExpectations.Setups.AllocateIndex(Arg.Any<int>()).ReturnValue(1);
        _ = writerExpectations.Setups.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult("snap-test-path"));
        await SnapshotOnceAsync(store, journal, writerExpectations.Instance(), cancellationToken);
    }

    private static async Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, ISnapshotWriter writer, CancellationToken cancellationToken)
    {
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var captureExpectations = new ISnapshotEntryCaptureCreateExpectations();
        _ = captureExpectations.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                               .ReturnValue(default);
        var exporterExpectations = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporterExpectations.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var throttleExpectations = new IBackgroundSnapshotMemoryThrottleCreateExpectations();
        _ = throttleExpectations.Setups.ShouldSuppressBackgroundSnapshot().ReturnValue(false);
        var deps = new CoordinatorDependencies(
            captureExpectations.Instance(),
            writer,
            store,
            exporterExpectations.Instance(),
            "test-node",
            throttleExpectations.Instance(),
            null);
        await new Coordinator(opt, journal, deps, TimeProvider.System).SnapshotAsync(journal, cancellationToken).ConfigureAwait(false);
    }
}
