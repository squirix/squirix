using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Tests for <see cref="JournalDurabilityGroupCommit" /> and durable mutation group-commit integration.
/// </summary>
public sealed class JournalDurabilityGroupCommitTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures canceling the only pending waiter leaves the next group commit batch usable.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GroupCommitCanceledOnlyWaiterDoesNotPoisonFutureBatch()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWaitMs = 25,
            JournalGroupCommitMaxBatch = 8,
        };

        var flushCounter = new AtomicCounter();

        var groupCommit = new JournalDurabilityGroupCommit(
            _ =>
            {
                flushCounter.Increment();
                return ValueTask.CompletedTask;
            },
            options);

        using var canceledCts = new CancellationTokenSource();

        var canceled = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(canceledCts.Token));
        await canceledCts.CancelAsync();

        await WaitUntilCompletedAsync(canceled);
        Assert.True(canceled.IsCanceled);

        await WaitUntilCompletedAsync(AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken)));

        Assert.Equal(1, flushCounter.Value);
    }

    /// <summary>
    /// Ensures a delayed flush failure fails pending waiters instead of leaving them pending.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GroupCommitDelayFlushFailureFailsPendingWaiters()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWaitMs = 5,
            JournalGroupCommitMaxBatch = 8,
        };
        var flushFailure = new InvalidOperationException("flush failed");
        var groupCommit = new JournalDurabilityGroupCommit(_ => throw flushFailure, options);

        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        await WaitUntilCompletedAsync(first);
        await WaitUntilCompletedAsync(second);
        var firstFailure = Assert.IsType<InvalidOperationException>(first.Exception?.InnerException);
        var secondFailure = Assert.IsType<InvalidOperationException>(second.Exception?.InnerException);

        Assert.Same(flushFailure, firstFailure);
        Assert.Same(flushFailure, secondFailure);
    }

    /// <summary>
    /// Ensures cancellation of the first waiter does not cancel the shared delayed flush for other waiters.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GroupCommitFirstWaiterCancellationDoesNotCancelOtherWaiters()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWaitMs = 25,
            JournalGroupCommitMaxBatch = 8,
        };

        var flushCounter = new AtomicCounter();

        var groupCommit = new JournalDurabilityGroupCommit(
            _ =>
            {
                flushCounter.Increment();
                return ValueTask.CompletedTask;
            },
            options);

        using var firstCts = new CancellationTokenSource();

        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(firstCts.Token));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        await firstCts.CancelAsync();

        await WaitUntilCompletedAsync(first);
        Assert.True(first.IsCanceled);

        await WaitUntilCompletedAsync(second);

        Assert.Equal(1, flushCounter.Value);
    }

    /// <summary>
    /// Ensures group commit still fsyncs before memory apply when enabled.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GroupCommitFsyncCompletesBeforeMemoryApply()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-group-commit-fsync");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 600_000,
                ManifestRetentionCount = 1,
                JournalGroupCommitMaxWaitMs = 2,
                JournalGroupCommitMaxBatch = 8,
            };
            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            var executor = new DurableMutationExecutor(journal);
            var observedPendingFlushDuringMemoryApply = false;

            _ = await executor.ExecuteAsync(
                "default:k",
                static _ => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
                async ct => { await journal.AppendPutAsync(CacheKey.Default("k"), DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null), null, ct); },
                _ =>
                {
                    observedPendingFlushDuringMemoryApply = journal.IsDurabilityFlushPending;
                    return new ValueTask<int>(1);
                },
                DefaultCancellationToken);

            Assert.False(observedPendingFlushDuringMemoryApply);
            Assert.False(journal.IsDurabilityFlushPending);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures concurrent durability waits share one flush when group commit is enabled.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task GroupCommitSharesFlushAcrossConcurrentWaiters()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-group-commit-batch");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 600_000,
                ManifestRetentionCount = 1,
                JournalGroupCommitMaxWaitMs = 50,
                JournalGroupCommitMaxBatch = 8,
            };

            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var flushProbe = new JournalFlushProbe(journal);
            var groupCommit = new JournalDurabilityGroupCommit(flushProbe.FlushAsync, options);

            await journal.AppendPutAsync(CacheKey.Default("k1"), DiscriminatedEntryJsonWriter.BuildEntryJson("v1", null, null, 1, null), null, DefaultCancellationToken);

            await journal.AppendPutAsync(CacheKey.Default("k2"), DiscriminatedEntryJsonWriter.BuildEntryJson("v2", null, null, 1, null), null, DefaultCancellationToken);

            var firstCommit = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
            var secondCommit = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
            await Task.WhenAll(firstCommit, secondCommit);

            Assert.Equal(1, flushProbe.FlushCount);
            Assert.False(journal.IsDurabilityFlushPending);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static Task AsSingleUseTaskAsync(ValueTask valueTask) => valueTask.AsTask();

    private static async Task WaitUntilCompletedAsync(Task task)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!task.IsCompleted && DateTime.UtcNow < deadline)
            await Task.Delay(TimeSpan.FromMilliseconds(10), DefaultCancellationToken).ConfigureAwait(false);

        Assert.True(task.IsCompleted);
    }

    private sealed class AtomicCounter
    {
        private int _value;

        public int Value => Volatile.Read(ref _value);

        public void Increment() => _ = Interlocked.Increment(ref _value);
    }

    private sealed class JournalFlushProbe
    {
        private readonly JournalWriter _journal;
        private int _flushCount;

        public JournalFlushProbe(JournalWriter journal)
        {
            _journal = journal;
        }

        public int FlushCount => Volatile.Read(ref _flushCount);

        public async ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _flushCount);
            await _journal.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
