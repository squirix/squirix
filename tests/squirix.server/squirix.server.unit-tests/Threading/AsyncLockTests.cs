using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the async lock hands ownership over in FIFO order and releases every queued waiter on disposal.</summary>
public sealed class AsyncLockTests : ServerUnitTestBase
{
    private const int StressOperationsPerWorker = 40;
    private const int StressRounds = 150;
    private const int StressWorkers = 8;

    /// <summary>A queued waiter that is canceled leaves the queue: the holder keeps the lock, and its release frees the lock instead of handing it to the canceled waiter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelRemovesQueuedWaiter(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waiter = asyncLock.LockAsync(waiterCancellation.Token);

        await waiterCancellation.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, AsyncLockHolder>(waiter);
        _ = await Assert.That(asyncLock.TryLock(out _, cancellationToken)).IsFalse();

        holder.Dispose();

        _ = await Assert.That(asyncLock.TryLock(out var next, cancellationToken)).IsTrue();
        next.Dispose();
        asyncLock.Dispose();
    }

    /// <summary>Disposing the lock faults a waiter parked on a token that can never be canceled instead of leaving it parked forever.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeFaultsWaiterOnNoneToken(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        var waiter = asyncLock.LockAsync(CancellationToken.None);
        _ = await Assert.That(waiter.IsCompleted).IsFalse();

        asyncLock.Dispose();

        _ = await Assert.That(waiter.IsFaulted).IsTrue();
        var disposed = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(waiter);
        _ = await Assert.That(disposed.ObjectName).IsEqualTo(nameof(AsyncLock));
        holder.Dispose();
    }

    /// <summary>Disposing the lock while its holder is still out does not revoke the holder: it keeps exclusion and releases without a throw.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLeavesCurrentHolderUsable(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        var waiter = asyncLock.LockAsync(cancellationToken);

        asyncLock.Dispose();
        asyncLock.Dispose();

        _ = await Assert.That(waiter.IsFaulted).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(waiter);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(asyncLock.LockAsync(cancellationToken));

        holder.Dispose();
        holder.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(asyncLock, cancellationToken, static (candidate, token) => { _ = candidate.TryLock(out _, token); });
    }

    /// <summary>Disposing the lock wakes a waiter parked on a cancelable token with ObjectDisposedException before the token fires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWakesGateWaiters(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = asyncLock.LockAsync(waiterCancellation.Token);
        var second = asyncLock.LockAsync(waiterCancellation.Token);
        _ = await Assert.That(first.IsCompleted).IsFalse();

        asyncLock.Dispose();

        _ = await Assert.That(first.IsFaulted).IsTrue();
        _ = await Assert.That(second.IsFaulted).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(first);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(second);
        holder.Dispose();
    }

    /// <summary>Try-locking a disposed lock throws ObjectDisposedException.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public void DisposedTryLockThrows(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(asyncLock, cancellationToken, static (candidate, token) => { _ = candidate.TryLock(out _, token); });
    }

    /// <summary>Releasing a holder after its lock was disposed under it (a bounded shutdown gave up waiting) is a no-op instead of a throw.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HolderReleaseAfterLockDisposeIsSafe(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        asyncLock.Dispose();

        holder.Dispose();
    }

    /// <summary>A default holder (what <c language="csharp">TryLock</c> returns while the lock is held) compares without throwing.</summary>
    [Test]
    public async Task DefaultHolderEqualsDoesNotThrow()
    {
        var first = default(AsyncLockHolder);
        var second = default(AsyncLockHolder);

        _ = await Assert.That(first.Equals(second)).IsTrue();
    }

    /// <summary>Concurrent lock, cancel and dispose interleavings keep exclusion and settle every waiter exactly once, with no lost wakeup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LockCancelDisposeInterleavingsSettle(CancellationToken cancellationToken)
    {
        for (var round = 0; round < StressRounds; round++)
            await RunStressRoundAsync(round % 2 == 0, cancellationToken);
    }

    /// <summary>Locking a disposed lock faults with ObjectDisposedException instead of waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LockAfterDisposeThrowsObjectDisposed(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(asyncLock.LockAsync(cancellationToken));
    }

    /// <summary>Releasing the lock hands ownership to queued waiters one at a time in arrival order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseHandsOffToNextWaiterFifo(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        var first = asyncLock.LockAsync(cancellationToken);
        var second = asyncLock.LockAsync(cancellationToken);
        var third = asyncLock.LockAsync(cancellationToken);

        holder.Dispose();
        var firstHolder = await first;
        _ = await Assert.That(second.IsCompleted).IsFalse();
        _ = await Assert.That(third.IsCompleted).IsFalse();

        firstHolder.Dispose();
        var secondHolder = await second;
        _ = await Assert.That(third.IsCompleted).IsFalse();

        secondHolder.Dispose();
        var thirdHolder = await third;
        _ = await Assert.That(asyncLock.TryLock(out _, cancellationToken)).IsFalse();

        thirdHolder.Dispose();
        _ = await Assert.That(asyncLock.TryLock(out var free, cancellationToken)).IsTrue();
        free.Dispose();
        asyncLock.Dispose();
    }

    /// <summary>Releasing the lock skips a canceled waiter in the middle of the queue and hands ownership to the next live waiter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseSkipsCanceledWaiter(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var middleCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = asyncLock.LockAsync(cancellationToken);
        var middle = asyncLock.LockAsync(middleCancellation.Token);
        var last = asyncLock.LockAsync(cancellationToken);

        await middleCancellation.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, AsyncLockHolder>(middle);

        holder.Dispose();
        var firstHolder = await first;
        _ = await Assert.That(last.IsCompleted).IsFalse();

        firstHolder.Dispose();
        var lastHolder = await last;
        lastHolder.Dispose();
        asyncLock.Dispose();
    }

    private static async Task RunStressRoundAsync(bool disposeMidway, CancellationToken cancellationToken)
    {
        var state = new StressState(new AsyncLock());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var workers = StartStressWorkers(state, cancellation.Token);

        await Task.Yield();
        await cancellation.CancelAsync();
        if (disposeMidway)
            state.Lock.Dispose();

        await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        state.Lock.Dispose();

        _ = await Assert.That(state.Violations).IsEqualTo(0);
        _ = await Assert.That(state.CountSettled()).IsEqualTo(StressWorkers * StressOperationsPerWorker);
    }

    private static async Task RunStressWorkerAsync(StressState state, CancellationToken token)
    {
        for (var operation = 0; operation < StressOperationsPerWorker; operation++)
        {
            try
            {
                using var holder = await state.Lock.LockAsync(token).ConfigureAwait(false);
                state.Enter();
                await Task.Yield();
                state.Exit();
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
            {
                state.Refuse();
            }
        }
    }

    private static Task[] StartStressWorkers(StressState state, CancellationToken cancelableToken)
    {
        var workers = new Task[StressWorkers];
        for (var worker = 0; worker < workers.Length; worker++)
            workers[worker] = RunStressWorkerAsync(state, worker % 2 == 0 ? cancelableToken : CancellationToken.None);

        return workers;
    }

    private sealed class StressState
    {
        private int _acquired;
        private int _inside;
        private int _refused;
        private int _violations;

        internal StressState(AsyncLock asyncLock)
        {
            Lock = asyncLock;
        }

        internal AsyncLock Lock { get; }

        internal int Violations => Volatile.Read(ref _violations);

        internal int CountSettled() => Volatile.Read(ref _acquired) + Volatile.Read(ref _refused);

        internal void Enter()
        {
            if (Interlocked.Increment(ref _inside) != 1)
                _ = Interlocked.Increment(ref _violations);
        }

        internal void Exit()
        {
            _ = Interlocked.Decrement(ref _inside);
            _ = Interlocked.Increment(ref _acquired);
        }

        internal void Refuse() => _ = Interlocked.Increment(ref _refused);
    }
}
