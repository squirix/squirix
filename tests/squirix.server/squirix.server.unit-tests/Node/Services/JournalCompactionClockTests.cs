using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The journal compaction <c language="text">MinGap</c> gate measures time on the server clock.</summary>
[Immutable]
public sealed class JournalCompactionClockTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan MinGap = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test");

    /// <summary>Compaction runs again once the server clock passes <c language="text">MinGap</c> since the last run, with no real delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunsAgainAfterMinGapOnServerClock(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(persistence, NullLogger<Ledger>.Instance);

        // An earlier snapshot in the manifest makes every compaction attempt eligible.
        await store.WriteAsync(
            new State { CurrentJournal = 1, NextSequence = 1, LastSnapshot = new SnapshotRef { Index = 1, Path = "snap-earlier", CreatedUtc = DateTime.UtcNow, ReplayFromJournalSegment = 1 } },
            cancellationToken);
        using var runs = new SemaphoreSlim(0);
        var maintenance = new IExclusiveMaintenanceExecutorCreateExpectations();
        _ = maintenance.Setups.ExecuteMaintenanceExclusiveAsync(Arg.Any<Func<CancellationToken, ValueTask>>(), Arg.Any<CancellationToken>())
                       .Callback((_, _) =>
                        {
                            _ = runs.Release();
                            return ValueTask.CompletedTask;
                        });
        var clock = new TimerSignalClock();
        await using var journal = new SnapshotCutJournal(1, 2);
        var cluster = new TopologyOptions([]) { ClusterId = "c", NodeId = "n", Uri = new Uri("https://localhost:1") };
        using var compaction = new JournalCompactionService<object?>(
            NullLogger<JournalCompactionService<object?>>.Instance,
            Options.Create(new JournalCompactionOptions { Enabled = true, MinGap = MinGap, MinTailBytes = 0, MinTailSegments = 0 }),
            new JournalCompactionDependencies(CreateCoordinator(journal, store), maintenance.Instance(), store, StoreFactory.CreateReader(), persistence, cluster, clock),
            new CompactionMetrics(_testMeter));

        await compaction.StartAsync(cancellationToken);
        var firstRun = await NextRunAsync(clock, runs, cancellationToken);
        var secondRun = await NextRunAsync(clock, runs, cancellationToken);
        await compaction.StopAsync(cancellationToken);

        _ = await Assert.That(firstRun).IsTrue();
        _ = await Assert.That(secondRun).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static Coordinator CreateCoordinator(SnapshotCutJournal journal, Ledger store)
    {
        var opt = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
        return new Coordinator(
            opt,
            journal,
            new CoordinatorDependencies(
                new ISnapshotEntryCaptureCreateExpectations().Instance(),
                new ISnapshotWriterCreateExpectations().Instance(),
                store,
                new IIdempotencySnapshotExporterCreateExpectations().Instance(),
                "n",
                new IBackgroundSnapshotMemoryThrottleCreateExpectations().Instance(),
                null));
    }

    /// <summary>Waits for the compaction loop to park on the clock, then advances past its wait and waits for the next run.</summary>
    /// <param name="clock">The server clock.</param>
    /// <param name="runs">Released once per compaction run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> when a compaction ran within the real-time bound.</returns>
    private static async Task<bool> NextRunAsync(TimerSignalClock clock, SemaphoreSlim runs, CancellationToken cancellationToken)
    {
        if (!await clock.TimerCreated.WaitAsync(Bound, cancellationToken))
            return false;

        // The wait is MinGap plus or minus a jitter of 10%, so this always reaches it.
        clock.Advance(MinGap * 1.2);
        return await runs.WaitAsync(Bound, cancellationToken);
    }

    /// <summary>A fake clock that signals each timer created on it, so a test advances only once a wait is armed.</summary>
    [ThreadSafe]
    private sealed class TimerSignalClock : FakeTimeProvider
    {
        internal TimerSignalClock()
            : base(DateTimeOffset.UtcNow)
        {
        }

        internal SemaphoreSlim TimerCreated { get; } = new(0);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = base.CreateTimer(callback, state, dueTime, period);
            _ = TimerCreated.Release();
            return timer;
        }
    }
}
