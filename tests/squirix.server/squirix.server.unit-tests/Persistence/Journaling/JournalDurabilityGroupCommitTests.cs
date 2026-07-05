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
using Squirix.Server.TestKit.Journaling;
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
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(25),
            JournalGroupCommitMaxBatch = 8,
        };

        var flushCounter = new AtomicCounter();
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushCounter.Increment, options, time);

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
        var groupCommit = CreateGroupCommit(() => throw flushFailure, options, time);

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
        var groupCommit = CreateGroupCommit(flushCounter.Increment, options, time);

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

    /// <summary>
    /// Ensures canceling a waiter after its batch was taken but before flush completion does not
    /// return the pooled waiter early and poison later durability waits.
    /// </summary>
    [Fact]
    public async Task GroupCommitCancelDuringInFlightBatchDoesNotPoisonPool()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 4,
        };

        var flushGate = new InFlightFlushGate(DefaultCancellationToken);
        try
        {
            var time = new FakeTimeProvider();
            var groupCommit = CreateGroupCommit(flushGate.BlockDuringFlush, options, time);

            using var firstCts = new CancellationTokenSource();
            var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(firstCts.Token));
            var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
            var third = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
            var fourth = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));

            var drainTask = Task.Factory.StartNew(
                groupCommit.DrainDueBatchesOnJournalThread,
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
            await drainTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);

            Assert.True(first.IsCanceled);

            var followUp = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(DefaultCancellationToken));
            time.Advance(options.JournalGroupCommitMaxWait);
            groupCommit.DrainDueBatchesOnJournalThread();
            await WaitUntilCompletedAsync(followUp);
            Assert.True(followUp.IsCompletedSuccessfully);
        }
        finally
        {
            flushGate.Dispose();
        }
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
        var observedPendingFlushDuringMemoryApply = new PendingFlushObservation();
        var key = CacheKey.Default("k");
        var payload = JournalEntryPayloadKit.EncodePut("v");

        _ = await executor.ExecuteAsync(
            key,
            static _ => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            journal,
            (Key: key, Payload: payload),
            static (j, append, ct) => j.AppendPutAsync(append.Key, append.Payload, ct),
            observedPendingFlushDuringMemoryApply,
            static (j, observation, _) =>
            {
                observation.PendingDuringApply = Assert.IsType<JournalCoordinator>(j).IsDurabilityFlushPending;
                return new ValueTask<int>(1);
            },
            DefaultCancellationToken);

        Assert.False(observedPendingFlushDuringMemoryApply.PendingDuringApply);
        Assert.False(Assert.IsType<JournalCoordinator>(journal).IsDurabilityFlushPending);
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
        var groupCommit = CreateGroupCommit(flushCounter.Increment, options, time);

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
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        await journal.AppendPutAsync(CacheKey.Default("k1"), JournalEntryPayloadKit.EncodePut("v1"), DefaultCancellationToken);
        await journal.AppendPutAsync(CacheKey.Default("k2"), JournalEntryPayloadKit.EncodePut("v2"), DefaultCancellationToken);

        var firstCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(DefaultCancellationToken));
        var secondCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(DefaultCancellationToken));
        await Task.WhenAll(firstCommit, secondCommit);

        Assert.False(pipelined.IsDurabilityFlushPending);
    }

    /// <summary>When the journal pipeline fails, pending group-commit durability waits fail instead of hanging.</summary>
    [Fact]
    public async Task JournalPipelineFailureFailsPendingGroupCommitDurabilityWait()
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

    private sealed class InFlightFlushGate(CancellationToken cancellationToken) : IDisposable
    {
        private readonly ManualResetEventSlim _flushEntered = new(false);
        private readonly ManualResetEventSlim _releaseFlush = new(false);

        public void BlockDuringFlush()
        {
            _flushEntered.Set();
            _releaseFlush.Wait(cancellationToken);
        }

        public bool WaitForFlushEntered(TimeSpan timeout) =>
            _flushEntered.Wait(timeout, cancellationToken);

        public void ReleaseFlush() => _releaseFlush.Set();

        public void Dispose()
        {
            _releaseFlush.Set();
            _releaseFlush.Dispose();
            _flushEntered.Dispose();
        }
    }

    private sealed class AtomicCounter
    {
        private int _value;

        public int Value => Volatile.Read(ref _value);

        public void Increment() => _ = Interlocked.Increment(ref _value);
    }

    private sealed class PendingFlushObservation
    {
        public bool PendingDuringApply { get; set; }
    }
}
