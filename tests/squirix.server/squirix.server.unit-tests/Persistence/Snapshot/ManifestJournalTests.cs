using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Regression tests for snapshot manifest journal-pointer consistency (issue #441).</summary>
[Immutable]
public sealed class ManifestJournalTests : IsolatedStorageTestBase
{
    /// <summary>
    /// When the manifest lags behind a segment roll (roll published after the snapshot capture),
    /// the published manifest carries the captured segment instead of the stale pointer.
    /// </summary>
    [Fact]
    public async Task StaleManifestKeepsCapturedSegment()
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir });
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, DefaultCancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Assert.Equal(2, published.CurrentJournal);
        Assert.Equal(2, published.LastSnapshot?.ReplayFromJournalSegment);
    }

    /// <summary>
    /// When a roll is published during the snapshot build, the published manifest never moves
    /// the journal pointer backward to the captured segment.
    /// </summary>
    [Fact]
    public async Task AheadManifestNeverMovesBackward()
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir });
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 3, NextSequence = 2 }, DefaultCancellationToken);
        await using var journal = new SnapshotCutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, DefaultCancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Assert.Equal(3, published.CurrentJournal);
        Assert.Equal(2, published.LastSnapshot?.ReplayFromJournalSegment);
    }

    private static async Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var captureExpectations = new ISnapshotEntryCaptureCreateExpectations();
        _ = captureExpectations.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>()).ReturnValue(default);
        var writerExpectations = new ISnapshotWriterCreateExpectations();
        _ = writerExpectations.Setups.WriteAsync(Arg.Any<int>(), Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult("snap-test-path"));
        var exporterExpectations = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporterExpectations.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var throttleExpectations = new IBackgroundSnapshotMemoryThrottleCreateExpectations();
        _ = throttleExpectations.Setups.ShouldSuppressBackgroundSnapshot().ReturnValue(false);
        var deps = new CoordinatorDependencies(
            captureExpectations.Instance(),
            writerExpectations.Instance(),
            store,
            exporterExpectations.Instance(),
            "test-node",
            throttleExpectations.Instance(),
            null);
        await new Coordinator(opt, journal, deps).SnapshotAsync(journal, cancellationToken).ConfigureAwait(false);
    }
}
