using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Tests for <see cref="JournalDurabilityGroupCommit" /> and durable mutation group-commit integration.
/// </summary>
public sealed class JournalDurabilityGroupCommitTests : ServerUnitTestBase
{
    /// <summary>Ensures canceling pending group-commit waiters propagates journal pipeline failures.</summary>
    [Fact]
    public async Task CancelPendingFailsPendingGroupCommitWaiters()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 8,
        };
        var failure = new IOException("journal pipeline failed");
        var groupCommit = CreateGroupCommit(static () => { }, options, new FakeTimeProvider());

        var waiter = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        await groupCommit.CancelPendingAsync(failure);
        await WaitUntilCompletedAsync(waiter);

        Assert.True(waiter.IsFaulted);
        Assert.Same(failure, waiter.Exception?.InnerException);
    }

    /// <summary>
    /// Ensures canceling a waiter after its batch was taken but before flush completion does not
    /// return the pooled waiter early and poison later durability waits.
    /// </summary>
    [Fact]
    public async Task GroupCommitCancelInFlightBatchDoesNotPoisonPool()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 4,
        };

        using var flushGate = new InFlightFlushGate(DefaultCancellationToken);
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushGate.BlockDuringFlushAction, options, time);

        using var firstCts = new CancellationTokenSource();
        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(firstCts.Token));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        var third = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        var fourth = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        var task = Task.Factory.StartNew(
            static state =>
            {
                if (state is not JournalDurabilityGroupCommit groupCommit)
                    throw new InvalidOperationException("Expected journal durability group commit state.");

                groupCommit.DrainDueBatchesOnJournalThread();
            },
            groupCommit,
            DefaultCancellationToken,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        Assert.True(flushGate.WaitForFlushEntered(TimeSpan.FromSeconds(5)));

        await firstCts.CancelAsync();
        flushGate.ReleaseFlush();

        await WaitUntilCompletedAsync(first);
        await WaitUntilCompletedAsync(second);
        await WaitUntilCompletedAsync(third);
        await WaitUntilCompletedAsync(fourth);
        await task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);

        Assert.True(first.IsCanceled);

        var followUp = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await WaitUntilCompletedAsync(followUp);
        Assert.True(followUp.IsCompletedSuccessfully);
    }

    /// <summary>Ensures canceling the only pending waiter leaves the next group commit batch usable.</summary>
    [Fact]
    public async Task GroupCommitCanceledWaiterDoesNotPoisonFutureBatch()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(25),
            JournalGroupCommitMaxBatch = 8,
        };

        var flushCounter = new AtomicCounter();
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushCounter.IncrementAction, options, time);

        using var canceledCts = new CancellationTokenSource();

        var canceled = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(canceledCts.Token));
        await canceledCts.CancelAsync();

        await WaitUntilCompletedAsync(canceled);
        Assert.True(canceled.IsCanceled);

        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await WaitUntilCompletedAsync(second);

        Assert.Equal(1, flushCounter.Value);
    }

    /// <summary>Ensures a delayed flush failure fails pending waiters instead of leaving them pending.</summary>
    [Fact]
    public async Task GroupCommitDelayFlushFailureFailsPendingWaiters()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(5),
            JournalGroupCommitMaxBatch = 8,
        };
        var flushFailure = new InvalidOperationException("flush failed");
        var time = new FakeTimeProvider();
        var failingFlush = new FailingFlush(flushFailure);
        var groupCommit = CreateGroupCommit(failingFlush.ThrowAction, options, time);

        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();

        await WaitUntilCompletedAsync(first);
        await WaitUntilCompletedAsync(second);
        var firstFailure = Assert.IsType<InvalidOperationException>(first.Exception?.InnerException);
        var secondFailure = Assert.IsType<InvalidOperationException>(second.Exception?.InnerException);

        Assert.Same(flushFailure, firstFailure);
        Assert.Same(flushFailure, secondFailure);
    }

    /// <summary>Ensures cancellation of the first waiter does not cancel the shared delayed flush for other waiters.</summary>
    [Fact]
    public async Task GroupCommitFirstWaiterCancelOtherWaiters()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(25),
            JournalGroupCommitMaxBatch = 8,
        };

        var flushCounter = new AtomicCounter();
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushCounter.IncrementAction, options, time);

        using var firstCts = new CancellationTokenSource();

        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(firstCts.Token));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        await firstCts.CancelAsync();

        await WaitUntilCompletedAsync(first);
        Assert.True(first.IsCanceled);

        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await WaitUntilCompletedAsync(second);

        Assert.Equal(1, flushCounter.Value);
    }

    /// <summary>Ensures group commit still fsyncs before memory apply when enabled.</summary>
    [Fact]
    public async Task GroupCommitFsyncCompletesBeforeMemoryApply()
    {
        using var dir = new TempDirectory("squirix-journal-group-commit-fsync");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(2),
            JournalGroupCommitMaxBatch = 8,
        };
        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var executor = new DurableMutationExecutor(journal);
        var key = CacheKey.Default("k");
        var payload = JournalEntryPayloadKit.EncodePut("v");
        var applyCount = new AtomicCounter();

        var applied = await executor.ExecuteAsync(
            key,
            static _ => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, ReadOnlyMemory<byte> Payload, AtomicCounter ApplyCount), int>(
                (journal, key, payload, applyCount),
                static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                static (s, _) =>
                {
                    s.ApplyCount.Increment();
                    return new ValueTask<int>(1);
                }),
            DefaultCancellationToken);

        Assert.Equal(1, applied);
        Assert.Equal(1, applyCount.Value);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken).AsTask();
    }

    /// <summary>Ensures an immediate batch flush racing the delay timer does not fail concurrent waiters.</summary>
    [Fact]
    public async Task GroupCommitImmediateBatchFlushRacesDelayTimer()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(50),
            JournalGroupCommitMaxBatch = 4,
        };

        var flushCounter = new AtomicCounter();
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushCounter.IncrementAction, options, time);

        var waiters = new Task[8];
        for (var i = 0; i < waiters.Length; i++)
            waiters[i] = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

        groupCommit.DrainDueBatchesOnJournalThread();
        groupCommit.DrainDueBatchesOnJournalThread();

        await Task.WhenAll(waiters);

        foreach (var waiter in waiters)
            Assert.True(waiter.IsCompletedSuccessfully);

        Assert.True(flushCounter.Value >= 1);
    }

    /// <summary>Ensures concurrent durability waits share one flush when group commit is enabled.</summary>
    [Fact]
    public async Task GroupCommitSharesFlushAcrossConcurrentWaiters()
    {
        using var dir = new TempDirectory("squirix-journal-group-commit-batch");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(50),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        await journal.AppendPutAsync(CacheKey.Default("k1"), JournalEntryPayloadKit.EncodePut("v1"), DefaultCancellationToken);
        await journal.AppendPutAsync(CacheKey.Default("k2"), JournalEntryPayloadKit.EncodePut("v2"), DefaultCancellationToken);

        var firstCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(DefaultCancellationToken));
        var secondCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(DefaultCancellationToken));
        await Task.WhenAll(firstCommit, secondCommit);

        Assert.True(firstCommit.IsCompletedSuccessfully);
        Assert.True(secondCommit.IsCompletedSuccessfully);
    }

    /// <summary>When the journal pipeline fails, pending group-commit durability waits fail instead of hanging.</summary>
    [Fact]
    public async Task JournalPipelineFailureFailsCommitDurabilityWait()
    {
        using var dir = new TempDirectory("squirix-journal-gc-pipeline-fail");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 32,
        };

        using var manifestStore = new ManifestStore(options);
        var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        try
        {
            await journal.AppendPutAsync(CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), DefaultCancellationToken);
            var durability = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(DefaultCancellationToken));
            await journal.DisposeAsync();
            await WaitUntilCompletedAsync(durability);
            Assert.True(durability.IsFaulted);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    private static Task AsSingleUseTaskAsync(ValueTask valueTask) => valueTask.AsTask();

    private static JournalDurabilityGroupCommit CreateGroupCommit(Action flush, PersistenceOptions options, FakeTimeProvider time) => new(flush, static () => { }, options, time);

    private static async Task WaitUntilCompletedAsync(Task task)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!task.IsCompleted && DateTime.UtcNow < deadline)
            await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, DefaultCancellationToken);

        Assert.True(task.IsCompleted);
    }

    private sealed class AtomicCounter
    {
        private int _value;

        internal AtomicCounter()
        {
            IncrementAction = Increment;
        }

        internal Action IncrementAction { get; }

        internal int Value => Volatile.Read(ref _value);

        internal void Increment() => _ = Interlocked.Increment(ref _value);
    }

    private sealed class FailingFlush
    {
        private readonly Exception _exception;

        internal FailingFlush(Exception exception)
        {
            _exception = exception;
            ThrowAction = Throw;
        }

        internal Action ThrowAction { get; }

        private void Throw() => throw _exception;
    }

    private sealed class InFlightFlushGate : IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly ManualResetEventSlim _flushEntered = new(false);
        private readonly ManualResetEventSlim _releaseFlush = new(false);

        internal InFlightFlushGate(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            BlockDuringFlushAction = BlockDuringFlush;
        }

        internal Action BlockDuringFlushAction { get; }

        public void Dispose()
        {
            _releaseFlush.Set();
            _releaseFlush.Dispose();
            _flushEntered.Dispose();
        }

        internal void ReleaseFlush() => _releaseFlush.Set();

        internal bool WaitForFlushEntered(TimeSpan timeout) => _flushEntered.Wait(timeout, _cancellationToken);

        private void BlockDuringFlush()
        {
            _flushEntered.Set();
            _releaseFlush.Wait(_cancellationToken);
        }
    }
}
