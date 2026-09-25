using System;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Concurrency and lifecycle tests for <see cref="JournalCompactionController" />.</summary>
[Immutable]
public sealed class JournalCompactionControllerTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test");

    /// <summary>Double dispose does not throw.</summary>
    [Test]
    [SuppressMessage("Major Code Smell", "S2699:Tests should include assertions", Justification = "This lifecycle test asserts that the second Dispose call does not throw.")]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Dispose must be called two times")]
    public async Task DisposeIsIdempotent()
    {
        var opt = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 1000 };
        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        using var controller = CreateController(opt, manifestStore, journal, CreateSnapshots(opt, manifestStore, journal));
        controller.Dispose();
    }

    /// <summary>Disposed controller rejects further compaction attempts.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TriggerAfterDisposeThrows(CancellationToken cancellationToken)
    {
        var opt = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 1000 };
        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        var controller = CreateController(opt, manifestStore, journal, CreateSnapshots(opt, manifestStore, journal));
        controller.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(controller.TryTriggerAsync(cancellationToken));
    }

    /// <summary>When the controller compaction mutex is already held, <see cref="JournalCompactionController.TryTriggerAsync" /> returns false without waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TriggerNowFalseWhenMutexUnavailableAsync(CancellationToken cancellationToken)
    {
        var opt = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 16,
            FlushInterval = 1000,
        };

        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        await journal.AppendPutUnderGateAsync(CacheKey.Default("gate"), JournalEntryPayloadKit.EncodePut("x"), cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        using var controller = CreateController(opt, manifestStore, journal, CreateSnapshots(opt, manifestStore, journal));

        var firstTrigger = controller.TryTriggerAsync(cancellationToken);
        var secondTrigger = controller.TryTriggerAsync(cancellationToken);
        var firstResult = await firstTrigger;
        var secondResult = await secondTrigger;

        _ = await Assert.That(firstResult ^ secondResult).IsTrue();
        _ = await Assert.That(await controller.TryTriggerAsync(cancellationToken)).IsTrue();
    }

    /// <summary>
    /// A trigger while a snapshot waits for its checkpoint ack is skipped without compacting, because a compaction publishing before the
    /// snapshot's manifest write would delete the segments it replays from; once the snapshot has published, the trigger compacts.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TriggerSkipsWhileSnapshotInFlight(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var snapshots = CreateSnapshots(journal.Journal.Options, journal.Ledger, journal.Journal);
        var maintenance = new RecordingMaintenanceExecutor();
        using var controller = CreateController(journal.Journal.Options, journal.Ledger, maintenance, snapshots);

        // An unflushed frame makes the cut's checkpoint issue a real fsync, which is what the stall blocks.
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        journal.Writer.Flush.Arm();
        var snapshot = snapshots.SnapshotAsync(journal.Journal, cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var triggeredDuringSnapshot = await controller.TryTriggerAsync(cancellationToken);
        var compactedDuringSnapshot = maintenance.Runs;
        journal.Writer.Flush.Release();
        await snapshot;
        var triggeredAfterSnapshot = await controller.TryTriggerAsync(cancellationToken);
        var reservationFree = snapshots.TryEnterCompaction();
        snapshots.ExitCompaction();

        _ = await Assert.That(triggeredDuringSnapshot).IsFalse();
        _ = await Assert.That(compactedDuringSnapshot).IsEqualTo(0);
        _ = await Assert.That(triggeredAfterSnapshot).IsTrue();
        _ = await Assert.That(maintenance.Runs).IsEqualTo(1);
        _ = await Assert.That(reservationFree).IsTrue();
        _ = await Assert.That((await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken)).LastSnapshot?.Index).IsEqualTo(1);
    }

    /// <summary>A compaction that fails still releases the reservation, so a later snapshot is not refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedTriggerReleasesReservation(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var snapshots = CreateSnapshots(journal.Journal.Options, journal.Ledger, journal.Journal);
        var failure = new InvalidOperationException("compaction failed");
        using var controller = CreateController(journal.Journal.Options, journal.Ledger, new RecordingMaintenanceExecutor(failure), snapshots);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(controller.TryTriggerAsync(cancellationToken));
        var reservationFree = snapshots.TryEnterCompaction();
        snapshots.ExitCompaction();

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(reservationFree).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static JournalCompactionController CreateController(PersistenceOptions persistence, Ledger ledger, IExclusiveMaintenanceExecutor maintenance, Coordinator snapshots) =>
        new(persistence, ledger, StoreFactory.CreateReader(), maintenance, snapshots, NullLogger<JournalCompactionController>.Instance);

    private Coordinator CreateSnapshots(PersistenceOptions persistence, Ledger ledger, IJournalCoordinator journal)
    {
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var deps = new CoordinatorDependencies(
            new LocalCacheSnapshotCapture<object?>(new PhysicalCache<object?>()),
            StoreFactory.CreateWriter(persistence),
            ledger,
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
            "n",
            new BackgroundSnapshotMemoryThrottle(new StateEvaluator(Options.Create(new PressureOptions())), new MemoryUsageAccounting()),
            null);
        return new Coordinator(opt, journal, deps);
    }

    /// <summary>Maintenance executor that counts runs without compacting, or fails each run with a given error.</summary>
    [ThreadSafe]
    private sealed class RecordingMaintenanceExecutor : IExclusiveMaintenanceExecutor
    {
        private readonly Exception? _failure;
        private int _runs;

        internal RecordingMaintenanceExecutor(Exception? failure = null)
        {
            _failure = failure;
        }

        internal int Runs => Volatile.Read(ref _runs);

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
        {
            _ = action;
            _ = Interlocked.Increment(ref _runs);
            return _failure == null ? ValueTask.CompletedTask : ValueTask.FromException(_failure);
        }
    }
}
