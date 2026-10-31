using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A snapshot whose checkpoint flush is stalled publishes only after a successful ack.</summary>
[Immutable]
public sealed class CoordinatorStallTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test");

    /// <summary>A checkpoint ack faulted by a failing fsync aborts the snapshot: no manifest entry, no completion event.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAbortsWhenCheckpointAckFaults(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var snapshots = CreateCoordinator(journal);
        var completed = CountCompletions(snapshots);

        await StallNextFlushAsync(journal, cancellationToken);
        var snapshot = snapshots.SnapshotAsync(journal.Journal, cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(new IOException("simulated fsync failure"));

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(snapshot);
        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot).IsNull();
        _ = await Assert.That(Volatile.Read(ref completed.Value)).IsEqualTo(0);
        _ = await Assert.That(snapshots.IsInFlight).IsFalse();
    }

    /// <summary>A snapshot canceled while it waits for its checkpoint ack is not published, even though the flush later succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAbortsWhenAckWaitCanceled(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var snapshots = CreateCoordinator(journal);
        var completed = CountCompletions(snapshots);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await StallNextFlushAsync(journal, cancellationToken);
        var snapshot = snapshots.SnapshotAsync(journal.Journal, caller.Token).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await caller.CancelAsync();
        journal.Writer.Flush.Release();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(snapshot);
        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot).IsNull();
        _ = await Assert.That(Volatile.Read(ref completed.Value)).IsEqualTo(0);
    }

    /// <summary>A snapshot publishes while the node reports critical memory pressure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotPublishesWhileMemoryIsCritical(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var cache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>());
        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<object?>("v"), cancellationToken);
        var pressure = new CriticalMemoryPressureReader(new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 1 }), new MemoryUsageAccounting(cache));
        var snapshots = CreateCoordinator(journal, cache);
        var completed = CountCompletions(snapshots);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);

        _ = await snapshots.SnapshotAsync(journal.Journal, cancellationToken);

        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(pressure.IsCritical()).IsTrue();
        _ = await Assert.That(manifest.LastSnapshot).IsNotNull();
        _ = await Assert.That(Volatile.Read(ref completed.Value)).IsEqualTo(1);
    }

    /// <summary>Once a snapshot has published, the capture buffers hold no entry or idempotency record references.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureBuffersAreEmptyAfterPublish(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        List<(CacheKey Key, NodeCacheEntry<object?> Entry)>? items = null;
        var capture = new ISnapshotEntryCaptureCreateExpectations();
        _ = capture.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                   .Callback((buffer, _, _) =>
                    {
                        buffer.Add((CacheKey.Default("k"), new NodeCacheEntry<object?>("v")));
                        items = buffer;
                        return ValueTask.CompletedTask;
                    });
        var opt = ServerJsonSerializer.Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var deps = new CoordinatorDependencies(
            capture.Instance(),
            StoreFactory.CreateWriter(journal.Journal.Options),
            journal.Ledger,
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System),
            "n",
            NoOpSnapshotTelemetry.Instance);
        var snapshots = new Coordinator(opt, journal.Journal, deps, TimeProvider.System);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);

        _ = await snapshots.SnapshotAsync(journal.Journal, cancellationToken);

        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot).IsNotNull();
        _ = await Assert.That(items).IsNotNull();
        _ = await Assert.That(items!.Count).IsEqualTo(0);
    }

    /// <summary>A manifest rewritten while the snapshot waits for its checkpoint ack, as a compaction does, aborts the publish.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAbortsOnManifestChange(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var snapshots = CreateCoordinator(journal);
        var completed = CountCompletions(snapshots);

        await StallNextFlushAsync(journal, cancellationToken);
        var snapshot = snapshots.SnapshotAsync(journal.Journal, cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var moved = new State
        {
            CurrentJournal = 2,
            NextSequence = 2,
            LastSnapshot = new SnapshotRef { Index = 7, CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 2 },
        };
        await journal.Ledger.WriteAsync(moved, cancellationToken);
        journal.Writer.Flush.Release();
        _ = await snapshot;

        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot?.Index).IsEqualTo(7);
        _ = await Assert.That(Volatile.Read(ref completed.Value)).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static StrongBox<int> CountCompletions(Coordinator snapshots)
    {
        var completed = new StrongBox<int>(0);
        snapshots.SnapshotCompleted += (_, _) => Interlocked.Increment(ref completed.Value);
        return completed;
    }

    private static async Task StallNextFlushAsync(StallableJournal journal, CancellationToken cancellationToken)
    {
        // An unflushed frame makes the cut's checkpoint issue a real fsync, which is what the stall blocks.
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        journal.Writer.Flush.Arm();
    }

    private Coordinator CreateCoordinator(StallableJournal journal, PhysicalCache<object?>? existingCache = null)
    {
        var persistence = journal.Journal.Options;
        var opt = ServerJsonSerializer.Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var cache = existingCache ?? new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>());
        var deps = new CoordinatorDependencies(
            new LocalCacheSnapshotCapture<object?>(cache),
            StoreFactory.CreateWriter(persistence),
            journal.Ledger,
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System),
            "n",
            NoOpSnapshotTelemetry.Instance);
        return new Coordinator(opt, journal.Journal, deps, TimeProvider.System);
    }
}
