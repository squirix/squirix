using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>A snapshot whose checkpoint flush is stalled publishes only after a successful ack and never overlaps a journal compaction.</summary>
[Immutable]
public sealed class CoordinatorStallTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan CompactionProbeWindow = TimeSpan.FromSeconds(1);

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
            LastSnapshot = new SnapshotRef { Index = 7, Path = "snap-foreign", CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 2 },
        };
        await journal.Ledger.WriteAsync(moved, cancellationToken);
        journal.Writer.Flush.Release();
        await snapshot;

        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot?.Index).IsEqualTo(7);
        _ = await Assert.That(Volatile.Read(ref completed.Value)).IsEqualTo(0);
    }

    /// <summary>A compaction attempt while a snapshot waits for its checkpoint ack is skipped; it runs once the snapshot has published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionSkipsWhileSnapshotInFlight(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);

        // An earlier snapshot in the manifest makes every periodic compaction attempt eligible.
        await journal.Ledger.WriteAsync(
            new State { CurrentJournal = 1, NextSequence = 1, LastSnapshot = new SnapshotRef { Index = 1, Path = "snap-earlier", CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 1 } },
            cancellationToken);
        var snapshots = CreateCoordinator(journal);
        var maintenance = new RecordingMaintenanceExecutor();
        var options = Options.Create(new JournalCompactionOptions { Enabled = true, MinGap = TimeSpan.FromMilliseconds(100), MinTailBytes = 0, MinTailSegments = 0 });
        var cluster = new TopologyOptions([]) { ClusterId = "c", NodeId = "n", Uri = new Uri("https://localhost:1") };
        using var compaction = new JournalCompactionService<object?>(
            NullLogger<JournalCompactionService<object?>>.Instance,
            options,
            new JournalCompactionDependencies(snapshots, maintenance, journal.Ledger, StoreFactory.CreateReader(), journal.Journal.Options, cluster),
            new CompactionMetrics(_testMeter));

        await StallNextFlushAsync(journal, cancellationToken);
        var snapshot = snapshots.SnapshotAsync(journal.Journal, cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await compaction.StartAsync(cancellationToken);
        var compactedDuringSnapshot = await StallableJournal.CompletesWithinAsync(maintenance.Entered, CompactionProbeWindow, cancellationToken);
        journal.Writer.Flush.Release();
        await snapshot;
        await maintenance.Entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await compaction.StopAsync(cancellationToken);

        _ = await Assert.That(compactedDuringSnapshot).IsFalse();
        var manifest = await journal.Ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot?.Index).IsEqualTo(2);
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

    private Coordinator CreateCoordinator(StallableJournal journal)
    {
        var persistence = journal.Journal.Options;
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var deps = new CoordinatorDependencies(
            new LocalCacheSnapshotCapture<object?>(new PhysicalCache<object?>()),
            StoreFactory.CreateWriter(persistence),
            journal.Ledger,
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
            "n",
            new BackgroundSnapshotMemoryThrottle(new StateEvaluator(Options.Create(new PressureOptions())), new MemoryUsageAccounting()),
            null);
        return new Coordinator(opt, journal.Journal, deps);
    }

    private sealed class RecordingMaintenanceExecutor : IExclusiveMaintenanceExecutor
    {
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
        {
            _ = action;
            _ = Entered.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}
