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

/// <summary>Verifies an async lock ownership holds only for its own acquisition of its own lock, until that acquisition releases.</summary>
public sealed class AsyncLockOwnershipTests : ServerUnitTestBase
{
    private const string RefusalMessage = "the guarded surface must hold the lock.";

    /// <summary>A canceled queued waiter leaves the holder's ownership holding, and the next queued waiter gets a holding ownership on the hand-off.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledWaiterKeepsHolderOwnership(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var canceled = asyncLock.LockAsync(cancel.Token);
        var waiter = asyncLock.LockAsync(cancellationToken);

        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, AsyncLockHolder>(canceled);
        _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(waiter.IsCompleted).IsFalse();

        holder.Dispose();
        var next = await waiter;
        _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsFalse();
        _ = await Assert.That(next.Ownership.Holds(asyncLock)).IsTrue();
        next.Dispose();
        asyncLock.Dispose();
    }

    /// <summary>A default ownership, and the one a default holder yields, never holds the lock, whether it is free or held.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultOwnershipNeverHolds(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        _ = await Assert.That(default(AsyncLockOwnership).Holds(asyncLock)).IsFalse();
        _ = await Assert.That(default(AsyncLockHolder).Ownership.Holds(asyncLock)).IsFalse();

        var holder = await asyncLock.LockAsync(cancellationToken);

        _ = await Assert.That(default(AsyncLockOwnership).Holds(asyncLock)).IsFalse();
        _ = await Assert.That(default(AsyncLockHolder).Ownership.Holds(asyncLock)).IsFalse();
        holder.Dispose();
        asyncLock.Dispose();
    }

    /// <summary>A FIFO hand-off moves the ownership at once: the released holder's ownership stops holding before the waiter resumes, and the waiter's holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HandOffMovesOwnershipToWaiter(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        var waiter = asyncLock.LockAsync(cancellationToken);
        var released = holder.Ownership;

        holder.Dispose();
        _ = await Assert.That(released.Holds(asyncLock)).IsFalse();

        var next = await waiter;
        _ = await Assert.That(released.Holds(asyncLock)).IsFalse();
        _ = await Assert.That(next.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(next.Ownership).IsNotEqualTo(released);

        next.Dispose();
        _ = await Assert.That(next.Ownership.Holds(asyncLock)).IsFalse();
        asyncLock.Dispose();
    }

    /// <summary>An ownership holds its lock from the acquisition until the holder releases, and never again once another acquisition takes the lock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnershipHoldsUntilRelease(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var first = await asyncLock.LockAsync(cancellationToken);
        var ownership = first.Ownership;
        _ = await Assert.That(ownership.Holds(asyncLock)).IsTrue();

        first.Dispose();
        _ = await Assert.That(ownership.Holds(asyncLock)).IsFalse();

        var second = await asyncLock.LockAsync(cancellationToken);
        _ = await Assert.That(ownership.Holds(asyncLock)).IsFalse();
        _ = await Assert.That(second.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(second.Ownership).IsNotEqualTo(ownership);
        second.Dispose();
        asyncLock.Dispose();
    }

    /// <summary>An ownership of one lock never holds another lock, even one held by an acquisition of the same generation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnershipOfAnotherLockDoesNotHold(CancellationToken cancellationToken)
    {
        var first = new AsyncLock();
        var second = new AsyncLock();
        var firstHolder = await first.LockAsync(cancellationToken);
        var secondHolder = await second.LockAsync(cancellationToken);

        _ = await Assert.That(firstHolder.Ownership.Holds(first)).IsTrue();
        _ = await Assert.That(firstHolder.Ownership.Holds(second)).IsFalse();
        _ = await Assert.That(secondHolder.Ownership.Holds(first)).IsFalse();
        secondHolder.Dispose();
        firstHolder.Dispose();
        second.Dispose();
        first.Dispose();
    }

    /// <summary>Disposing the lock does not revoke the holder: its ownership keeps holding until the holder releases.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnershipSurvivesLockDispose(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        var ownership = holder.Ownership;

        asyncLock.Dispose();
        _ = await Assert.That(ownership.Holds(asyncLock)).IsTrue();

        holder.Dispose();
        _ = await Assert.That(ownership.Holds(asyncLock)).IsFalse();
    }

    /// <summary>The guard passes the holder's ownership and refuses a released or default one with the caller's message while another flow holds the lock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowIfNotHeldRefusesNonHolder(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var first = await asyncLock.LockAsync(cancellationToken);
        var stale = first.Ownership;
        first.Dispose();
        var holder = await asyncLock.LockAsync(cancellationToken);

        holder.Ownership.ThrowIfNotHeld(asyncLock, RefusalMessage);
        var refused = NodeExceptionAssert.For<InvalidOperationException>().Throws(stale, asyncLock, static (ownership, candidate) => ownership.ThrowIfNotHeld(candidate, RefusalMessage));
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(asyncLock, static candidate => default(AsyncLockOwnership).ThrowIfNotHeld(candidate, RefusalMessage));

        _ = await Assert.That(refused.Message).IsEqualTo(RefusalMessage);
        holder.Dispose();
        asyncLock.Dispose();
    }
}
