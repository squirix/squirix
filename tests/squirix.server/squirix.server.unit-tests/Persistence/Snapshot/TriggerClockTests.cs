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
using Squirix.Server.TestKit;
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

    private const string DefaultOptions = """{"minGapBetweenSnapshots":"00:01:00","snapshotEveryNOps":10}""";

    private static readonly TimeSpan MinGap = TimeSpan.FromMinutes(1);

    /// <summary>An ops threshold reached within the min gap waits, and a snapshot is taken once the server clock passes the gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinGapElapsesOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        var withinGap = published.Count;
        clock.Advance(MinGap);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

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
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        clock.Advance(MinGap);
        clock.StepWallClock(TimeSpan.FromMinutes(-30));
        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(published.Count).IsEqualTo(2);
    }

    /// <summary>At the journal high-water mark a snapshot is taken inside the min gap once a closed segment is left to free.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerFiresInsideMinGap(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        journal.CurrentSegmentIndex = 4;
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(published.Count).IsEqualTo(2);
    }

    /// <summary>The fill trigger waits while the last snapshot already covers every closed segment.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerNeedsNewClosedSegment(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(published.Count).IsEqualTo(1);
    }

    /// <summary>Below the high-water mark the min gap still holds a snapshot back although a segment rolled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFillTriggerBelowHighWater(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        journal.CurrentSegmentIndex = 4;
        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That(published.Count).IsEqualTo(1);
    }

    /// <summary>The fill trigger waits for a snapshot that is still in flight instead of starting a second one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerWaitsForSnapshotInFlight(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        var gate = new WriterGate();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken, DefaultOptions, gate);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        var first = coordinator.SnapshotAsync(journal, cancellationToken).AsTask();
        await gate.Reached.WaitAsync(cancellationToken);
        journal.CurrentSegmentIndex = 4;
        var second = await coordinator.SnapshotAsync(journal, cancellationToken);
        gate.Release();
        var firstPublished = await first;

        _ = await Assert.That((second, firstPublished, published.Count)).IsEqualTo((false, true, 1));
    }

    /// <summary>The fill trigger ignores the latency throttle that holds back every other trigger.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerIgnoresLatencyThrottle(CancellationToken cancellationToken)
    {
        const string throttled = """{"minGapBetweenSnapshots":"00:01:00","snapshotEveryNOps":10,"latencySloMilliseconds":10}""";
        var clock = new SteppedWallClock();
        var metrics = new CountingJournalMetrics();
        metrics.SetLatency(100d);
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var (coordinator, published) = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken, throttled);
        await using var journal = new SnapshotCutJournal(3, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);
        var throttledCount = published.Count;
        journal.AtHighWater = true;
        _ = await coordinator.SnapshotAsync(journal, cancellationToken);

        _ = await Assert.That((throttledCount, published.Count)).IsEqualTo((0, 1));
    }

    private static async Task<(Coordinator Coordinator, PublishCounter Published)> CreateCoordinatorAsync(
        Ledger store,
        IJournalMetrics metrics,
        TimeProvider clock,
        CancellationToken cancellationToken,
        string options = DefaultOptions,
        WriterGate? gate = null)
    {
        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var opt = ServerJsonSerializer.Deserialize<TriggerOptions>(options)!;
        var capture = new ISnapshotEntryCaptureCreateExpectations();
        _ = capture.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                   .ReturnValue(default);
        var writer = new ISnapshotWriterCreateExpectations();
        _ = writer.Setups.AllocateIndex(Arg.Any<int>()).Callback(static publishedIndex => publishedIndex + 1);
        _ = writer.Setups.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).Callback((_, _, _, token) => gate == null ? ValueTask.FromResult("snap-test-path") : gate.WaitAsync(token));
        var exporter = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporter.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var coordinator = new Coordinator(
            opt,
            metrics,
            new CoordinatorDependencies(capture.Instance(), writer.Instance(), store, exporter.Instance(), "test-node", NoOpSnapshotTelemetry.Instance),
            clock);
        var published = new PublishCounter();
        coordinator.SnapshotCompleted += published.OnCompleted;
        return (coordinator, published);
    }

    /// <summary>Journal metrics whose append counters a test advances by hand.</summary>
    [ThreadSafe]
    private sealed class CountingJournalMetrics : IJournalMetrics
    {
        private double _latency;
        private long _ops;

        public long AppendedBytes => Interlocked.Read(ref _ops) * 100;

        public long AppendedOps => Interlocked.Read(ref _ops);

        public double RecentAppendLatencyMs => Volatile.Read(ref _latency);

        internal void SetLatency(double milliseconds) => Volatile.Write(ref _latency, milliseconds);

        internal void Append(long ops) => _ = Interlocked.Add(ref _ops, ops);
    }

    /// <summary>Holds the snapshot writer until the test releases it.</summary>
    [ThreadSafe]
    private sealed class WriterGate
    {
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Reached => _reached.Task;

        internal void Release() => _ = _release.TrySetResult();

        internal async ValueTask<string> WaitAsync(CancellationToken cancellationToken)
        {
            _ = _reached.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return "snap-test-path";
        }
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
