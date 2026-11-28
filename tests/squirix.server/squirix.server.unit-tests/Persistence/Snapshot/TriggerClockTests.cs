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
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>The snapshot trigger measures its min gap and interval on the monotonic server clock.</summary>
[Immutable]
public sealed class TriggerClockTests : IsolatedStorageTestBase
{
    private const string DefaultOptions = """{"minGapBetweenSnapshots":"00:01:00","snapshotEveryNOps":10}""";
    private const long OpsPerSnapshot = 10;

    private static readonly TimeSpan MinGap = TimeSpan.FromMinutes(1);

    /// <summary>A backward wall-clock step after a snapshot does not hold back the next one once the gap passed on monotonic time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BackwardWallStepKeepsOpsTrigger(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
        clock.Advance(MinGap);
        clock.StepWallClock(TimeSpan.FromMinutes(-30));
        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
    }

    /// <summary>At the journal high-water mark a snapshot is taken inside the min gap once a closed segment is left to free.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerFiresInsideMinGap(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
        journal.CurrentSegmentIndex = 4;
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
    }

    /// <summary>The fill trigger ignores the latency throttle that holds back every other trigger.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerIgnoresLatencyThrottle(CancellationToken cancellationToken)
    {
        const string throttled = """{"minGapBetweenSnapshots":"00:01:00","snapshotEveryNOps":10,"latencySloMilliseconds":10}""";
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        metrics.SetLatency(100d);
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken, throttled);
        await using var journal = new SnapshotCutJournal(3, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsFalse();
        journal.AtHighWater = true;
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
    }

    /// <summary>The fill trigger waits while the last snapshot already covers every closed segment.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerNeedsNewClosedSegment(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsFalse();
    }

    /// <summary>The fill trigger waits for a snapshot that is still in flight instead of starting a second one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FillTriggerWaitsForSnapshotInFlight(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        var gate = new WriterGate();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken, DefaultOptions, gate);
        await using var journal = new SnapshotCutJournal(3, 2) { AtHighWater = true };

        var first = coordinator.SnapshotAsync(journal, cancellationToken).AsTask();
        await gate.Reached.WaitAsync(cancellationToken);
        journal.CurrentSegmentIndex = 4;
        var second = await coordinator.SnapshotAsync(journal, cancellationToken);
        gate.Release();
        var firstPublished = await first;

        _ = await Assert.That((second, firstPublished)).IsEqualTo((false, true));
    }

    /// <summary>An ops threshold reached within the min gap waits, and a snapshot is taken once the server clock passes the gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinGapElapsesOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(1, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsFalse();
        clock.Advance(MinGap);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
    }

    /// <summary>Below the high-water mark the min gap still holds a snapshot back although a segment rolled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFillTriggerBelowHighWater(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(DateTimeOffset.UtcNow);
        var metrics = new CountingJournalMetrics();
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir }, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var coordinator = await CreateCoordinatorAsync(store, metrics, clock, cancellationToken);
        await using var journal = new SnapshotCutJournal(3, 2);

        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsTrue();
        journal.CurrentSegmentIndex = 4;
        metrics.Append(OpsPerSnapshot);
        _ = await Assert.That(await coordinator.SnapshotAsync(journal, cancellationToken)).IsFalse();
    }

    private static async Task<Coordinator> CreateCoordinatorAsync(
        Ledger store,
        IJournalMetrics metrics,
        TimeProvider clock,
        CancellationToken cancellationToken,
        string options = DefaultOptions,
        WriterGate? gate = null)
    {
        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var opt = ServerJsonSerializer.Deserialize<TriggerOptions>(options)!;
        var capture = ISnapshotEntryCapture.Mock(MockBehavior.Strict);
        _ = capture.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                   .ReturnsAsync(ValueTask.CompletedTask);
        var writer = ISnapshotWriter.Mock(MockBehavior.Strict);
        _ = writer.AllocateIndex(Arg.Any<int>()).Returns(static publishedIndex => publishedIndex + 1);
        _ = writer.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).ReturnsAsync((_, _, _, token) => gate?.WaitAsync(token) ?? ValueTask.FromResult("snap-test-path"));
        return new Coordinator(
            opt,
            metrics,
            new CoordinatorDependencies(capture.Object, writer.Object, store, "test-node", NoOpSnapshotTelemetry.Instance),
            clock);
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

        internal void Append(long ops) => _ = Interlocked.Add(ref _ops, ops);

        internal void SetLatency(double milliseconds) => Volatile.Write(ref _latency, milliseconds);
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
}
