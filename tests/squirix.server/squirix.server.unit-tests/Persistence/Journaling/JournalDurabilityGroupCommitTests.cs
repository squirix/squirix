using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Tests for <see cref="JournalDurabilityGroupCommit" /> and durable mutation group-commit integration.</summary>
[Immutable]
public sealed class JournalDurabilityGroupCommitTests : IsolatedStorageTestBase
{
    /// <summary>Ensures waits admitted after CancelPending fail fast instead of parking on a dead batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AwaitCommitAfterCancelPendingThrows(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 8,
        };
        var groupCommit = CreateGroupCommit(static () => { }, options, new FakeTimeProvider());
        _ = groupCommit.CancelPending(new ObjectDisposedException(nameof(JournalDurabilityGroupCommit)));

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(groupCommit.AwaitCommitAsync(cancellationToken));
    }

    /// <summary>
    /// Ensures canceling an ack after its batch was taken but before flush completion does not
    /// break the in-flight batch or later durability waits.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelInFlightBatchKeepsCommitsUsable(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 4,
        };

        using var flushGate = new InFlightFlushGate(cancellationToken);
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushGate.BlockDuringFlushAction, options, time);

        using var firstCts = new CancellationTokenSource();
        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(firstCts.Token));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        var third = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        var fourth = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));

        var task = Task.Factory.StartNew(
            static state =>
            {
                if (state is not JournalDurabilityGroupCommit groupCommit)
                    throw new InvalidOperationException("Expected journal durability group commit state.");

                groupCommit.DrainDueBatchesOnJournalThread();
            },
            groupCommit,
            cancellationToken,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        _ = await Assert.That(flushGate.WaitForFlushEntered(TimeSpan.FromSeconds(5))).IsTrue();

        await firstCts.CancelAsync();
        flushGate.ReleaseFlush();

        await first.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await second.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await third.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await fourth.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(first.IsCanceled).IsTrue();
        _ = await Assert.That(second.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(third.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(fourth.IsCompletedSuccessfully).IsTrue();

        var followUp = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await followUp.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(followUp.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Ensures canceling pending group-commit acks propagates journal pipeline failures.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelPendingFailsPendingGroupCommitAcks(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 8,
        };
        var failure = new IOException("journal pipeline failed");
        var groupCommit = CreateGroupCommit(static () => { }, options, new FakeTimeProvider());

        var ack = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        _ = groupCommit.CancelPending(failure);
        await ack.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);

        _ = await Assert.That(ack.IsFaulted).IsTrue();
        _ = await Assert.That(ack.Exception?.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Canceling pending acks also faults the batch whose flush is in flight; the late flush outcome is a no-op.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelPendingFaultsInFlightBatch(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 1,
        };
        var failure = new ObjectDisposedException(nameof(JournalCoordinator));
        using var flushGate = new InFlightFlushGate(cancellationToken);
        var groupCommit = CreateGroupCommit(flushGate.BlockDuringFlushAction, options, new FakeTimeProvider());

        var ack = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        var drain = Task.Factory.StartNew(
            static state =>
            {
                if (state is not JournalDurabilityGroupCommit groupCommit)
                    throw new InvalidOperationException("Expected journal durability group commit state.");

                groupCommit.DrainDueBatchesOnJournalThread();
            },
            groupCommit,
            cancellationToken,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        _ = await Assert.That(flushGate.WaitForFlushEntered(TimeSpan.FromSeconds(5))).IsTrue();

        var faulted = groupCommit.CancelPending(failure);
        var repeated = groupCommit.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator)));
        await ack.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        flushGate.ReleaseFlush();
        await drain.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(faulted).IsEqualTo(1);
        _ = await Assert.That(repeated).IsEqualTo(0);
        _ = await Assert.That(ack.Exception?.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Ensures canceling the only pending ack leaves the next group commit batch usable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledAckDoesNotPoisonFutureBatch(CancellationToken cancellationToken)
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

        await canceled.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(canceled.IsCanceled).IsTrue();

        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await second.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);

        _ = await Assert.That(flushCounter.Value).IsEqualTo(1);
    }

    /// <summary>Ensures a delayed flush failure fails pending acks and is rethrown so the journal pipeline fails.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DelayFlushFailureFailsWaiters(CancellationToken cancellationToken)
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

        var first = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));

        time.Advance(options.JournalGroupCommitMaxWait);
        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws(groupCommit, static g => g.DrainDueBatchesOnJournalThread());

        await first.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        await second.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(thrown).IsSameReferenceAs(flushFailure);
        var firstFailure = await Assert.That(first.Exception?.InnerException).IsTypeOf<InvalidOperationException>();
        var secondFailure = await Assert.That(second.Exception?.InnerException).IsTypeOf<InvalidOperationException>();

        _ = await Assert.That(firstFailure).IsSameReferenceAs(flushFailure);
        _ = await Assert.That(secondFailure).IsSameReferenceAs(flushFailure);
    }

    /// <summary>Ensures group commit still fsyncs before memory apply when enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FsyncCompletesBeforeMemoryApply(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(2),
            JournalGroupCommitMaxBatch = 8,
        };
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var executor = new DurableMutationExecutor(journal);
        var key = CacheKey.Default("k");
        var payload = JournalEntryPayloadKit.EncodePut("v");
        var applyCount = new AtomicCounter();

        var applied = await executor.ExecuteAsync(
            key,
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, ReadOnlyMemory<byte> Payload, AtomicCounter ApplyCount), int>(
                (journal, key, payload, applyCount),
                static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                static (s, _) =>
                {
                    s.ApplyCount.Increment();
                    return new ValueTask<int>(1);
                }),
            cancellationToken);

        _ = await Assert.That(applied).IsEqualTo(1);
        _ = await Assert.That(applyCount.Value).IsEqualTo(1);
        await journal.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
    }

    /// <summary>Ensures cancellation of the first ack does not cancel the shared delayed flush for other acks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupCommitFirstAckCancelOtherAcks(CancellationToken cancellationToken)
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
        var second = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));

        await firstCts.CancelAsync();

        await first.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
        _ = await Assert.That(first.IsCanceled).IsTrue();

        time.Advance(options.JournalGroupCommitMaxWait);
        groupCommit.DrainDueBatchesOnJournalThread();
        await second.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);

        _ = await Assert.That(flushCounter.Value).IsEqualTo(1);
    }

    /// <summary>Ensures an immediate batch flush racing the delay timer does not fail concurrent acks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ImmediateFlushRacesDelayTimer(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(50),
            JournalGroupCommitMaxBatch = 4,
        };

        var flushCounter = new AtomicCounter();
        var time = new FakeTimeProvider();
        var groupCommit = CreateGroupCommit(flushCounter.IncrementAction, options, time);

        var acks = new Task[8];
        for (var i = 0; i < acks.Length; i++)
            acks[i] = AsSingleUseTaskAsync(groupCommit.AwaitCommitAsync(cancellationToken));

        groupCommit.DrainDueBatchesOnJournalThread();
        groupCommit.DrainDueBatchesOnJournalThread();

        await Task.WhenAll(acks);

        foreach (var ack in acks)
            _ = await Assert.That(ack.IsCompletedSuccessfully).IsTrue();

        _ = await Assert.That(flushCounter.Value >= 1).IsTrue();
    }

    /// <summary>Ensures concurrent durability waits share one flush when group commit is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneFlushSharedByConcurrentAcks(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(50),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        await journal.AppendPutUnderGateAsync(CacheKey.Default("k1"), JournalEntryPayloadKit.EncodePut("v1"), cancellationToken);
        await journal.AppendPutUnderGateAsync(CacheKey.Default("k2"), JournalEntryPayloadKit.EncodePut("v2"), cancellationToken);

        var firstCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(cancellationToken));
        var secondCommit = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(cancellationToken));
        await Task.WhenAll(firstCommit, secondCommit);

        _ = await Assert.That(firstCommit.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(secondCommit.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>When the journal pipeline fails, pending group-commit durability waits fail instead of hanging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PipelineFailureFailsDurabilityWait(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30),
            JournalGroupCommitMaxBatch = 32,
        };

        using var manifestStore = new Ledger(options);
        var journal = JournalCoordinatorFactory.Create(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true));

        try
        {
            await journal.AppendPutUnderGateAsync(CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
            var durability = AsSingleUseTaskAsync(journal.AwaitDurabilityCommitAsync(cancellationToken));
            await journal.DisposeAsync();
            await durability.WaitUntilAsync(static t => t.IsCompleted, cancellationToken);
            _ = await Assert.That(durability.IsFaulted).IsTrue();
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    private static Task AsSingleUseTaskAsync(ValueTask valueTask) => valueTask.AsTask();

    private static JournalDurabilityGroupCommit CreateGroupCommit(Action flush, PersistenceOptions options, FakeTimeProvider time) => new(flush, static () => { }, options, time);

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

    [Immutable]
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

    [Immutable]
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
