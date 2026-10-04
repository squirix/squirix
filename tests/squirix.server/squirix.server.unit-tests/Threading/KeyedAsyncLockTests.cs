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

/// <summary>Verifies the keyed async lock grants one key in FIFO order, keeps distinct keys independent and leaves no entry behind.</summary>
public sealed class KeyedAsyncLockTests : ServerUnitTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Waiters of one key are granted in arrival order, one at a time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SameKeyIsGrantedInArrivalOrder(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<string>();
        var first = await locks.LockAsync("k", cancellationToken);
        var second = locks.LockAsync("k", cancellationToken);
        var third = locks.LockAsync("k", cancellationToken);

        _ = await Assert.That(second.IsCompleted).IsFalse();
        _ = await Assert.That(third.IsCompleted).IsFalse();

        first.Dispose();
        var secondLease = await second.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(third.IsCompleted).IsFalse();

        secondLease.Dispose();
        (await third.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)).Dispose();
        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    /// <summary>A held key does not delay another key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DistinctKeysDoNotWait(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<string>();
        using var held = await locks.LockAsync("a", cancellationToken);

        var other = locks.LockAsync("b", cancellationToken);

        _ = await Assert.That(other.IsCompletedSuccessfully).IsTrue();
        (await other).Dispose();
    }

    /// <summary>A queued waiter that is cancelled leaves the queue and the table is empty once the holder releases.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledWaiterLeavesTable(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<string>();
        var holder = await locks.LockAsync("k", cancellationToken);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waiter = locks.LockAsync("k", waiterCancellation.Token);
        var survivor = locks.LockAsync("k", cancellationToken);

        await waiterCancellation.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, KeyedAsyncLock<string>.Lease>(waiter);

        _ = await Assert.That(locks.Count).IsEqualTo(1);

        holder.Dispose();
        (await survivor.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)).Dispose();

        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    /// <summary>A token that is already cancelled never creates an entry that outlives the call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledBeforeAcquireLeavesNothing(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<string>();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, KeyedAsyncLock<string>.Lease>(locks.LockAsync("k", cancelled.Token));

        _ = await Assert.That(locks.Count).IsEqualTo(0);
        (await locks.LockAsync("k", cancellationToken)).Dispose();
    }

    /// <summary>The lease can be released from another thread, which hands the key to the queued waiter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseOnAnotherThreadHandsOff(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<string>();
        var holder = await locks.LockAsync("k", cancellationToken);
        var waiter = locks.LockAsync("k", cancellationToken);

        await Task.Factory.StartNew(holder.Dispose, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        (await waiter.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)).Dispose();

        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }
}
