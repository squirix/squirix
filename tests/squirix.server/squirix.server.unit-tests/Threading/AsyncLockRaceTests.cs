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

/// <summary>Verifies lock handoff ordering against queued cancellation and disposal.</summary>
public sealed class AsyncLockRaceTests : ServerUnitTestBase
{
    private const int SharedCancellationRounds = 128;
    private const int WaiterCount = 8;

    /// <summary>Removing a canceled queue prefix preserves the next two live acquisitions and their order.</summary>
    /// <param name="prefixLength">The number of canceled waiters preceding the live waiters.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    public async Task CanceledHeadKeepsNextWaiter(int prefixLength, CancellationToken cancellationToken)
    {
        using var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var prefix = new Task<AsyncLockHolder>[prefixLength];
        for (var index = 0; index < prefix.Length; index++)
            prefix[index] = asyncLock.LockAsync(canceled.Token).AsTask();
        var first = asyncLock.LockAsync(CancellationToken.None);
        var second = asyncLock.LockAsync(CancellationToken.None);

        await canceled.CancelAsync();
        foreach (var waiter in prefix)
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(waiter, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsTrue();
        holder.Dispose();

        _ = await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        var firstHolder = await first;
        _ = await Assert.That(firstHolder.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(second.IsCompleted).IsFalse();
        firstHolder.Dispose();
        _ = await Assert.That(second.IsCompletedSuccessfully).IsTrue();
        (await second).Dispose();
    }

    /// <summary>A handoff may win after the token is canceled but before its queued cancellation callback executes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HandoffWinsBeforeCancelCallback(CancellationToken cancellationToken)
    {
        using var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = asyncLock.LockAsync(canceled.Token);
        var second = asyncLock.LockAsync(CancellationToken.None);

        // Cancellation callbacks run last-in-first-out: hand off before the lock's own callback can cancel this waiter.
        await using var release = canceled.Token.UnsafeRegister(
            static state =>
            {
                if (state is AsyncLockHolder current)
                    _ = current.TryRelease();
            },
            holder);

        await canceled.CancelAsync();

        _ = await Assert.That(canceled.IsCancellationRequested).IsTrue();
        _ = await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        var firstHolder = await first;
        _ = await Assert.That(firstHolder.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsFalse();
        holder.Dispose();
        _ = await Assert.That(firstHolder.Ownership.Holds(asyncLock)).IsTrue();
        _ = await Assert.That(second.IsCompleted).IsFalse();
        firstHolder.Dispose();
        _ = await Assert.That(second.IsCompletedSuccessfully).IsTrue();
        (await second).Dispose();
    }

    /// <summary>Every order of handoff, cancellation and disposal preserves the winning result and releases all waiters.</summary>
    /// <param name="caseIndex">The action order: release, cancel and dispose in each of their six permutations.</param>
    /// <param name="firstOutcome">The first waiter's expected result.</param>
    /// <param name="secondOutcome">The second waiter's expected result.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The scenario contains an unknown action.</exception>
    [Test]
    [Arguments(0, "granted", "disposed")]
    [Arguments(1, "granted", "disposed")]
    [Arguments(2, "canceled", "granted")]
    [Arguments(3, "canceled", "disposed")]
    [Arguments(4, "disposed", "disposed")]
    [Arguments(5, "disposed", "disposed")]
    public async Task HandoffCancelDisposeOrders(int caseIndex, string firstOutcome, string secondOutcome, CancellationToken cancellationToken)
    {
        string[] orders = ["RCD", "RDC", "CRD", "CDR", "DRC", "DCR"];
        using var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = asyncLock.LockAsync(canceled.Token);
        var second = asyncLock.LockAsync(CancellationToken.None);

        foreach (var action in orders[caseIndex])
        {
            switch (action)
            {
                case 'R':
                    holder.Dispose();
                    break;
                case 'C':
                    await canceled.CancelAsync();
                    break;
                case 'D':
                    asyncLock.Dispose();
                    break;
                default:
                    throw new InvalidOperationException("Unknown lock action.");
            }
        }

        _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsFalse();
        await AssertOutcomeAsync(asyncLock, first, firstOutcome, cancellationToken);
        await AssertOutcomeAsync(asyncLock, second, secondOutcome, cancellationToken);
    }

    /// <summary>Canceling a shared token concurrently with handoff settles all queued waiters without revoking the winner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedCancellationRacesWithHandoff(CancellationToken cancellationToken)
    {
        for (var round = 0; round < SharedCancellationRounds; round++)
        {
            using var asyncLock = new AsyncLock();
            var holder = await asyncLock.LockAsync(cancellationToken);
            using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var waiters = new Task<AsyncLockHolder>[WaiterCount];
            for (var index = 0; index < waiters.Length; index++)
                waiters[index] = asyncLock.LockAsync(canceled.Token).AsTask();
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cancel = CancelAfterStartAsync(start.Task, canceled);
            var release = ReleaseAfterStartAsync(start.Task, holder);

            start.SetResult();
            await Task.WhenAll(cancel, release).WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            var granted = 0;
            foreach (var waiter in waiters)
            {
                _ = await Assert.That(waiter.IsCompleted).IsTrue();
                if (waiter.IsCompletedSuccessfully)
                {
                    granted++;
                    var winner = await waiter;
                    _ = await Assert.That(winner.Ownership.Holds(asyncLock)).IsTrue();
                    winner.Dispose();
                }
                else
                {
                    _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(waiter, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
                }
            }

            _ = await Assert.That(granted).IsLessThanOrEqualTo(1);
        }
    }

    private static async Task AssertOutcomeAsync(AsyncLock asyncLock, ValueTask<AsyncLockHolder> waiter, string outcome, CancellationToken cancellationToken)
    {
        _ = await Assert.That(waiter.IsCompleted).IsTrue();
        switch (outcome)
        {
            case "granted":
                _ = await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
                var holder = await waiter;
                _ = await Assert.That(holder.Ownership.Holds(asyncLock)).IsTrue();
                holder.Dispose();
                break;
            case "canceled":
                _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, AsyncLockHolder>(waiter, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
                break;
            case "disposed":
                _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(waiter, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unknown waiter outcome.");
        }
    }

    private static async Task CancelAfterStartAsync(Task start, CancellationTokenSource canceled)
    {
        await start.ConfigureAwait(false);
        await canceled.CancelAsync().ConfigureAwait(false);
    }

    private static async Task ReleaseAfterStartAsync(Task start, AsyncLockHolder holder)
    {
        await start.ConfigureAwait(false);
        holder.Dispose();
    }
}
