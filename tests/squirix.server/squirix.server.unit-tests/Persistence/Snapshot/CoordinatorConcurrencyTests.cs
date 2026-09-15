using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Concurrent trigger evaluation in the snapshot coordinator (issue #450 S4).</summary>
[Immutable]
public sealed class CoordinatorConcurrencyTests : IsolatedStorageTestBase
{
    /// <summary>Concurrent trigger invocations evaluate shared trigger state; exactly one snapshot is published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTriggersPublishExactlyOnce(CancellationToken cancellationToken)
    {
        using var store = new Ledger(new PersistenceOptions { DataDir = Dir });
        await store.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        await using var journal = new SnapshotCutJournal(1, 2);
        var captureExpectations = new ISnapshotEntryCaptureCreateExpectations();
        _ = captureExpectations.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                               .ReturnValue(default);
        var writerExpectations = new ISnapshotWriterCreateExpectations();
        _ = writerExpectations.Setups.WriteAsync(
            Arg.Any<int>(),
            Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
            Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
            Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult("snap-test-path"));
        var exporterExpectations = new IIdempotencySnapshotExporterCreateExpectations();
        _ = exporterExpectations.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
        var throttleExpectations = new IBackgroundSnapshotMemoryThrottleCreateExpectations();
        _ = throttleExpectations.Setups.ShouldSuppressBackgroundSnapshot().ReturnValue(false);
        var coordinator = new Coordinator(
            opt,
            journal,
            new CoordinatorDependencies(
                captureExpectations.Instance(),
                writerExpectations.Instance(),
                store,
                exporterExpectations.Instance(),
                "test-node",
                throttleExpectations.Instance(),
                null));
        var published = new StrongBox<int>(0);
        coordinator.SnapshotCompleted += (_, _) => Interlocked.Increment(ref published.Value);

        // A start gate releases every caller at once so all of them evaluate the shared trigger
        // state before the single-flight CAS admits a winner.
        using var gate = new ManualResetEventSlim(false);
        var tasks = StartSnapshotCallers(coordinator, journal, gate, 16, cancellationToken);

        gate.Set();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref published.Value)).IsEqualTo(1);
        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot?.Index).IsEqualTo(1);
    }

    private static async Task RunGatedSnapshotAsync(Coordinator coordinator, IJournalCoordinator journal, ManualResetEventSlim gate, CancellationToken cancellationToken)
    {
        _ = gate.Wait(TimeSpan.FromSeconds(5), cancellationToken);
        await coordinator.SnapshotAsync(journal, cancellationToken).ConfigureAwait(false);
    }

    private static Task[] StartSnapshotCallers(Coordinator coordinator, IJournalCoordinator journal, ManualResetEventSlim gate, int count, CancellationToken cancellationToken)
    {
        var tasks = new Task[count];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = StartCallerAsync();

        return tasks;

        Task StartCallerAsync()
        {
            return Task.Factory.StartNew(
                () => RunGatedSnapshotAsync(coordinator, journal, gate, cancellationToken),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }
    }
}
