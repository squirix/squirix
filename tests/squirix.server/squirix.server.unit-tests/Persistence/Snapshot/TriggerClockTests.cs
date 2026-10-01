using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>The snapshot trigger measures its min gap and interval on the monotonic server clock.</summary>
[Immutable]
public sealed class TriggerClockTests : IsolatedStorageTestBase
{
    private const long OpsPerSnapshot = 10;

    private static readonly TimeSpan MinGap = TimeSpan.FromMinutes(1);

    /// <summary>An ops threshold reached within the min gap waits, and a snapshot is taken once the server clock passes the gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinGapElapsesOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        await coordinator.SnapshotAsync(journal, cancellationToken);
        metrics.Append(OpsPerSnapshot);
        await coordinator.SnapshotAsync(journal, cancellationToken);
        var withinGap = published.Count;
        clock.Advance(MinGap);
        await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(withinGap).IsEqualTo(1);
        _ = await Assert.That(published.Count).IsEqualTo(2);
    }

    /// <summary>A backward wall-clock step after a snapshot does not hold back the next one once the gap passed on monotonic time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BackwardWallStepKeepsOpsTrigger(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        await coordinator.SnapshotAsync(journal, cancellationToken);
        clock.Advance(MinGap);
        clock.StepWallClock(TimeSpan.FromMinutes(-30));
        metrics.Append(OpsPerSnapshot);
        await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(published.Count).IsEqualTo(2);
    }

    private static async Task<(Coordinator Coordinator, PublishCounter Published)> CreateCoordinatorAsync(Ledger store, IJournalMetrics metrics, TimeProvider clock, CancellationToken cancellationToken)
    {
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:01:00","snapshotEveryNOps":10}""")!;
        var capture = new ISnapshotEntryCaptureCreateExpectations();
        _ = capture.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                   .ReturnValue(default);
        var writer = new ISnapshotWriterCreateExpectations();
        _ = writer.Setups.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult("snap-test-path"));
        var exporter = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporter.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var throttle = new IBackgroundSnapshotMemoryThrottleCreateExpectations();
        _ = throttle.Setups.ShouldSuppressBackgroundSnapshot().ReturnValue(false);
        var coordinator = new Coordinator(
            opt,
            metrics,
            new CoordinatorDependencies(capture.Instance(), writer.Instance(), store, exporter.Instance(), "test-node", throttle.Instance(), null),
            clock);
        var published = new PublishCounter();
        coordinator.SnapshotCompleted += published.OnCompleted;
        return (coordinator, published);
    }

    /// <summary>Journal metrics whose append counters a test advances by hand.</summary>
    [ThreadSafe]
    private sealed class CountingJournalMetrics : IJournalMetrics
    {
        private long _ops;

        public long AppendedBytes => Interlocked.Read(ref _ops) * 100;

        public long AppendedOps => Interlocked.Read(ref _ops);

        public double RecentAppendLatencyMs => 0;

        internal void Append(long ops) => _ = Interlocked.Add(ref _ops, ops);
    }

    /// <summary>Counts published snapshots.</summary>
    [ThreadSafe]
    private sealed class PublishCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal void OnCompleted(object? sender, CompletedEventArgs e) => _ = Interlocked.Increment(ref _count);
    }

    /// <summary>A fake clock whose wall time can step backward while its monotonic timestamp keeps moving forward only.</summary>
    [ThreadSafe]
    private sealed class SteppedWallClock : FakeTimeProvider
    {
        private long _wallOffsetTicks;

        internal SteppedWallClock()
            : base(DateTimeOffset.UtcNow)
        {
        }

        public override DateTimeOffset GetUtcNow() => base.GetUtcNow().AddTicks(Interlocked.Read(ref _wallOffsetTicks));

        /// <summary>Reads the unstepped time, since the base derives timestamps from the virtual wall time; the monotonic clock never moves back.</summary>
        /// <returns>The monotonic timestamp.</returns>
        public override long GetTimestamp() => base.GetUtcNow().UtcTicks;

        internal void StepWallClock(TimeSpan step) => _ = Interlocked.Add(ref _wallOffsetTicks, step.Ticks);
    }
}
