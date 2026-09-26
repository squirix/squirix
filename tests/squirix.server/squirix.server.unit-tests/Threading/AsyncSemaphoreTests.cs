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

/// <summary>Verifies the async semaphore counts permits, hands them over in FIFO order, and releases every queued waiter on disposal.</summary>
public sealed class AsyncSemaphoreTests : ServerUnitTestBase
{
    /// <summary>A queued waiter that is canceled leaves the queue, and the released permit becomes available instead of going to it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelRemovesQueuedWaiter(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        await semaphore.WaitAsync(cancellationToken);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waiter = semaphore.WaitAsync(waiterCancellation.Token);

        await waiterCancellation.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(waiter);

        semaphore.Release();
        await semaphore.WaitAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
    }

    /// <summary>Disposing the semaphore faults waiters parked on a token that can never be canceled and on a cancelable one, instead of leaving them parked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeFaultsQueuedWaiters(CancellationToken cancellationToken)
    {
        var semaphore = new AsyncSemaphore(1);
        await semaphore.WaitAsync(cancellationToken);
        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var onNone = semaphore.WaitAsync(CancellationToken.None);
        var onCancelable = semaphore.WaitAsync(waiterCancellation.Token);
        _ = await Assert.That(onNone.IsCompleted || onCancelable.IsCompleted).IsFalse();

        semaphore.Dispose();

        var disposed = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(onNone);
        _ = await Assert.That(disposed.ObjectName).IsEqualTo(nameof(AsyncSemaphore));
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(onCancelable);
    }

    /// <summary>A permit held when the semaphore is disposed stays valid: returning it never throws, and new waits are refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeldPermitReleaseAfterDisposeIsSafe(CancellationToken cancellationToken)
    {
        var semaphore = new AsyncSemaphore(1);
        await semaphore.WaitAsync(cancellationToken);

        semaphore.Dispose();
        semaphore.Dispose();
        semaphore.Release();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(semaphore.WaitAsync(cancellationToken));
    }

    /// <summary>The semaphore hands out as many permits as it was created with, then queues the next wait until one is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PermitsAreCounted(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(2);
        await semaphore.WaitAsync(cancellationToken);
        await semaphore.WaitAsync(cancellationToken);
        var third = semaphore.WaitAsync(cancellationToken);
        _ = await Assert.That(third.IsCompleted).IsFalse();

        semaphore.Release();

        await third;
    }

    /// <summary>Releasing hands the permit to queued waiters one at a time in arrival order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseHandsOffToNextWaiterFifo(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        await semaphore.WaitAsync(cancellationToken);
        var first = semaphore.WaitAsync(cancellationToken);
        var second = semaphore.WaitAsync(cancellationToken);

        semaphore.Release();
        await first;
        _ = await Assert.That(second.IsCompleted).IsFalse();

        semaphore.Release();
        await second;
    }

    /// <summary>A semaphore needs at least one permit.</summary>
    [Test]
    public void ZeroPermitsAreRefused() => _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(0, static permits => new AsyncSemaphore(permits).Dispose());
}
