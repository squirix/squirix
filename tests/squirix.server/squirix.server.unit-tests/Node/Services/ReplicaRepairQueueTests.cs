using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The queue of demoted follower slots that wakes the readiness service.</summary>
[Immutable]
public sealed class ReplicaRepairQueueTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A slot queued while the service waits ends the wait at once, long before the delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedSlotEndsTheWait(CancellationToken cancellationToken)
    {
        var queue = new ReplicaRepairQueue(3);
        var wait = queue.WaitAsync(TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        queue.Enqueue(2);

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>A slot queued twice before it is taken is taken once, and can be queued again afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlotIsQueuedOnceUntilTaken(CancellationToken cancellationToken)
    {
        var queue = new ReplicaRepairQueue(3);
        queue.Enqueue(2);
        queue.Enqueue(2);
        queue.Enqueue(1);

        _ = await Assert.That(await queue.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That(await queue.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsFalse();

        queue.Enqueue(2);
        _ = await Assert.That(await queue.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>With nothing queued the wait ends when the delay elapses on the given clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DelayEndsAnEmptyWait(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var queue = new ReplicaRepairQueue(3);
        var wait = queue.WaitAsync(TimeSpan.FromSeconds(5), clock, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(5));

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>Canceling the token ends the wait by throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsTheWait(CancellationToken cancellationToken)
    {
        var queue = new ReplicaRepairQueue(3);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = queue.WaitAsync(TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token);

        await canceled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
    }
}
