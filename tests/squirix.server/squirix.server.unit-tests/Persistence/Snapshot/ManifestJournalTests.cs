using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Threading;
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
        await using var journal = new CutJournal(2, 2);

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
        await using var journal = new CutJournal(2, 2);

        await SnapshotOnceAsync(store, journal, DefaultCancellationToken);

        var published = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Assert.Equal(3, published.CurrentJournal);
        Assert.Equal(2, published.LastSnapshot?.ReplayFromJournalSegment);
    }

    private static async Task SnapshotOnceAsync(Ledger store, IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var deps = new CoordinatorDependencies(
            new EmptyEntryCapture(),
            new FixedPathWriter(),
            store,
            new EmptyIdempotencyExporter(),
            "test-node",
            new AllowBackgroundSnapshots(),
            null);
        await new Coordinator(opt, journal, deps).TrySnapshotAsync(journal, cancellationToken).ConfigureAwait(false);
    }

    private sealed class AllowBackgroundSnapshots : IBackgroundSnapshotMemoryThrottle
    {
        public bool ShouldSuppressBackgroundSnapshot() => false;
    }

    private sealed class CutJournal : IJournalCoordinator
    {
        private EventHandler? _onAppended;

        internal CutJournal(int segmentIndex, ulong nextSequence)
        {
            CurrentSegmentIndex = segmentIndex;
            NextSequence = nextSequence;
        }

        public event EventHandler? OnAppended
        {
            add => _onAppended += value;
            remove => _onAppended -= value;
        }

        public long AppendedBytes => 1024;

        public long AppendedOps => 1;

        public int CurrentSegmentIndex { get; }

        public bool HasFlushLoopFailure => false;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate => new();

        public bool IsJournalGroupCommitEnabled => false;

        public long MaxBytes => 0;

        public ulong NextSequence { get; }

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => default;

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => default;

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => default;

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => default;

        public ValueTask DisposeAsync() => default;

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => default;

        public async ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken)
        {
            var barrier = await captureUnderBarrier(state, 1, cancellationToken).ConfigureAwait(false);
            return await buildOutsideBarrier(state, 1, barrier, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) => default;

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => default;

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) => default;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => default;
    }

    private sealed class EmptyEntryCapture : ISnapshotEntryCapture
    {
        public ValueTask CaptureEntriesAsync(List<(CacheKey Key, NodeCacheEntry<object?> Entry)> target, DateTime utcNow, CancellationToken cancellationToken) => default;
    }

    private sealed class EmptyIdempotencyExporter : IIdempotencySnapshotExporter
    {
        public void ExportSnapshot(List<PersistedIdempotencyRecord> destination, DateTime utcNow)
        {
        }
    }

    private sealed class FixedPathWriter : ISnapshotWriter
    {
        public ValueTask<string> WriteAsync(
            int index,
            IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
            IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
            CancellationToken cancellationToken) => ValueTask.FromResult("snap-test-path");
    }
}
