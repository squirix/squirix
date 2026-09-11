using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Racing appends against disposal must settle explicitly: every published frame is flushed by
/// disposal (durable) or its append fails with <see cref="ObjectDisposedException" />. Nothing
/// may hang, and no acknowledged frame may be missing from the journal. The durability wait past
/// publication is best effort: shutdown may fail it routinely without un-counting the frame.
/// </summary>
[Immutable]
public sealed class JournalShutdownQuiescenceTests : IsolatedStorageTestBase
{
    /// <summary>Appends issued after disposal fail explicitly instead of hanging or vanishing.</summary>
    [Fact]
    public async Task AppendAfterDisposeThrowsObjectDisposed()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);

        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        var payload = JournalEntryPayloadKit.EncodePut("v");
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(journal.AppendPutAsync(CacheKey.Default("late"), payload, DefaultCancellationToken));
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(journal, static j => _ = j.AwaitDurabilityCommitAsync(DefaultCancellationToken).AsTask());
    }

    /// <summary>Canceling a flush before its checkpoint enters the ring must not leak the ack into the registry.</summary>
    [Fact]
    public async Task CanceledFlushLeavesRegistryEmpty()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(cts.Token));

        Assert.Empty(coordinator.DurabilityAcks.TakeAll(new ObjectDisposedException(nameof(JournalCoordinator))));
    }

    /// <summary>A marker wait with no budget left aborts disposal loudly instead of hanging.</summary>
    [Fact]
    public async Task MarkerTimeoutAbortsDisposalLoudly()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var failures = new List<Exception>();
        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException>(coordinator.DurabilityPipeline.EnqueueShutdownMarkerAsync(failures, cts.Token));

        var failure = Assert.Single(failures);
        _ = Assert.IsType<TimeoutException>(failure);
    }

    /// <summary>A join with no budget left records the timeout instead of hanging disposal.</summary>
    [Fact]
    public async Task JoinTimeoutRecordsFailure()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        var failures = new List<Exception>();
        await coordinator.DurabilityPipeline.AwaitJournalThreadDuringDisposeAsync(failures, TimeSpan.Zero);

        var failure = Assert.Single(failures);
        _ = Assert.IsType<TimeoutException>(failure);
    }

    /// <summary>A zero-budget join attempt reports the live thread without waiting.</summary>
    [Fact]
    public async Task JoinTimeoutReturnsFalse()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        Assert.False(await coordinator.DurabilityPipeline.TryJoinJournalThreadAsync(TimeSpan.Zero));
    }

    /// <summary>Appends racing disposal on the group-commit path settle explicitly without loss.</summary>
    [Fact]
    public Task ShutdownRaceSettlesAppendsGroupCommit() => RaceAppendsAgainstShutdownAsync(Dir, TimeSpan.FromMilliseconds(2), DefaultCancellationToken);

    /// <summary>Appends racing disposal on the strict fsync path settle explicitly without loss.</summary>
    [Fact]
    public Task ShutdownRaceSettlesAppendsStrict() => RaceAppendsAgainstShutdownAsync(Dir, TimeSpan.Zero, DefaultCancellationToken);

    private static async Task AppendLoopAsync(IJournalCoordinator journal, byte[] payload, int writer, int ops, HashSet<string> successes, Lock gate)
    {
        for (var i = 0; i < ops; i++)
        {
            var key = $"q{writer}-{i}";
            try
            {
                // Phase 1 (counted): publish the frame. The gate orders it ahead of the shutdown
                // marker, so disposal flushes it: durable. ODE means shutdown won the race.
                await journal.AppendPutAsync(CacheKey.Default(key), payload, CancellationToken.None);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            lock (gate)
                _ = successes.Add(key);

            // Phase 2 (best effort): cover durability. ODE here is routine during shutdown and
            // must not un-count the append above (e.g. every batch waiter failed at once).
            try
            {
                await journal.AwaitDurabilityCommitAsync(CancellationToken.None);
            }
            catch (ObjectDisposedException ex)
            {
                TestLog.Suppressed("Durability wait raced disposal; the append above stays counted.", ex);
            }
        }
    }

    private static async Task RaceAppendsAgainstShutdownAsync(string dataDir, TimeSpan groupCommitMaxWait, CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = dataDir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = groupCommitMaxWait,
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);

        const int writers = 8;
        const int opsPerWriter = 50;
        var payload = JournalEntryPayloadKit.EncodePut("v");
        var successes = new HashSet<string>(StringComparer.Ordinal);
        var gate = new Lock();
        var traffic = StartAppendTrafficAsync(journal, payload, writers, opsPerWriter, successes, gate);

        // Start disposal as soon as the first append is admitted: writers still have the rest of
        // their operations ahead, so the shutdown marker deterministically lands mid-traffic
        // instead of relying on a fixed delay.
        // Negative control, kept as documentation: with the gate check neutered, the strict
        // variant fails via disk-miss in ~50ms and the group-commit variant trips the 30s hang
        // guard, so this test does exercise the overlap it asserts.
        var spinDeadline = Environment.TickCount64 + 10_000;
        while (journal.AppendedOps == 0 && Environment.TickCount64 < spinDeadline)
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, cancellationToken).ConfigureAwait(false);

        Assert.True(journal.AppendedOps > 0);

        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        // Any hang here fails the test via timeout: no waiter may park forever.
        await traffic.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        var found = new HashSet<string>(StringComparer.Ordinal);
        using var records = JournalReadPath.ReadAll(dataDir, 1, cancellationToken);
        while (records.MoveNext())
        {
            if (records.Current.Operation == JournalOperationKind.Put)
                _ = found.Add(records.Current.Key.Key);
        }

        foreach (var key in successes)
            Assert.True(found.Contains(key), $"acknowledged append '{key}' is missing from the journal.");

        // Self-check, not luck: polling above guarantees at least one admitted append, and an
        // admitted append always completes (or trips the hang guard), so zero successes would mean
        // the shutdown overlap never happened rather than a passing test.
        Assert.True(successes.Count > 0, "the race admitted no appends; the shutdown overlap was not exercised.");
    }

    private static Task StartAppendTrafficAsync(IJournalCoordinator journal, byte[] payload, int writers, int opsPerWriter, HashSet<string> successes, Lock gate)
    {
        var tasks = new Task[writers];
        for (var w = 0; w < writers; w++)
            tasks[w] = AppendLoopAsync(journal, payload, w, opsPerWriter, successes, gate);

        return Task.WhenAll(tasks);
    }
}
