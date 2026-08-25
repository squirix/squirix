using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Shutdown behavior for snapshot-triggered journal compaction.</summary>
[Immutable]
public sealed class JournalCompactionServiceShutdownTests : IsolatedStorageTestBase
{
    /// <summary>Compaction started after a snapshot is canceled when the host stops.</summary>
    [Fact]
    public async Task ShutdownClearsSnapshotCompactionFlight()
    {
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 1000 };
        using var store = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            store,
            new JournalStartupGate());
        await journal.AppendPutAsync(CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var cluster = new TopologyOptions([]) { ClusterId = "c", NodeId = "n", Uri = new Uri("https://localhost:1") };
        var maintenance = new BlockingMaintenanceExecutor();
        var cache = new PhysicalCache<object?>();
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        var options = Options.Create(new JournalCompactionOptions { Enabled = true, MinGap = TimeSpan.Zero, MinTailBytes = 0, MinTailSegments = 0 });
        var deps = new CoordinatorDependencies(
            new LocalCacheSnapshotCapture<object?>(cache),
            StoreFactory.CreateWriter(persistence),
            store,
            new RpcMutationIdempotencyStore(),
            cluster.NodeId,
            new BackgroundSnapshotMemoryThrottle(new StateEvaluator(Options.Create(new PressureOptions())), new MemoryUsageAccounting()),
            null);
        var snapshots = new Coordinator(opt, journal, deps);
        using var compaction = new JournalCompactionService<object?>(
            NullLogger<JournalCompactionService<object?>>.Instance,
            options,
            new JournalCompactionDependencies(snapshots, maintenance, store, StoreFactory.CreateReader(persistence), persistence, cluster));

        await compaction.StartAsync(DefaultCancellationToken);
        await snapshots.TrySnapshotAsync(journal, DefaultCancellationToken);
        await maintenance.Entered.WaitAsync(DefaultCancellationToken);
        Assert.True(compaction.IsInFlight);

        await compaction.StopAsync(DefaultCancellationToken);

        Assert.False(compaction.IsInFlight);
    }

    [Immutable]
    private sealed class BlockingMaintenanceExecutor : IExclusiveMaintenanceExecutor
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Entered => _entered.Task;

        public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
        {
            _ = action;
            _ = _entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
    }
}
