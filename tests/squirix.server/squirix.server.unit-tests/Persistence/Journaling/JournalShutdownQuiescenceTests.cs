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
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Racing appends against disposal must settle explicitly: every append either completes durably or
/// fails with <see cref="ObjectDisposedException" />. Nothing may hang, and no acknowledged write
/// may be missing from the journal.
/// </summary>
[Immutable]
public sealed class JournalShutdownQuiescenceTests : IsolatedStorageTestBase
{
    /// <summary>Appends racing disposal on the strict fsync path settle explicitly without loss.</summary>
    [Fact]
    public Task ShutdownRaceSettlesAppendsStrict() => RaceAppendsAgainstShutdownAsync(Dir, TimeSpan.Zero, DefaultCancellationToken);

    /// <summary>Appends racing disposal on the group-commit path settle explicitly without loss.</summary>
    [Fact]
    public Task ShutdownRaceSettlesAppendsGroupCommit() => RaceAppendsAgainstShutdownAsync(Dir, TimeSpan.FromMilliseconds(2), DefaultCancellationToken);

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
        };

        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(DefaultCancellationToken);
        var coordinator = Assert.IsType<JournalCoordinator>(journal);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(journal.AwaitDurabilityCommitAsync(cts.Token));

        Assert.Empty(coordinator.DurabilityAcks.TakeAll());
    }

    private static Task StartAppendTrafficAsync(IJournalCoordinator journal, byte[] payload, int writers, int opsPerWriter, HashSet<string> successes, Lock gate)
    {
        var tasks = new Task[writers];
        for (var w = 0; w < writers; w++)
            tasks[w] = AppendLoopAsync(journal, payload, w, opsPerWriter, successes, gate);

        return Task.WhenAll(tasks);
    }

    private static async Task AppendLoopAsync(IJournalCoordinator journal, byte[] payload, int writer, int ops, HashSet<string> successes, Lock gate)
    {
        for (var i = 0; i < ops; i++)
        {
            var key = $"q{writer}-{i}";
            try
            {
                await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default(key), payload, CancellationToken.None);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            lock (gate)
                _ = successes.Add(key);
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
    }
}
