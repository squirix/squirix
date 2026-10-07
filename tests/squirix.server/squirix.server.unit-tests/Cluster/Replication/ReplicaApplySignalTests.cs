using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The signal that wakes the apply loop of a follower group.</summary>
[Immutable]
public sealed class ReplicaApplySignalTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A notification raised before the wait ends it at once, long before the delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotifyBeforeWaitEndsItAtOnce(CancellationToken cancellationToken)
    {
        var signal = new ReplicaApplySignal();
        signal.Notify();

        var wait = signal.WaitAsync(TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);

        _ = await Assert.That(wait.IsCompleted).IsTrue();
        _ = await Assert.That(await wait).IsTrue();
    }

    /// <summary>A notification raised while the loop waits ends the wait at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotifyDuringWaitEndsIt(CancellationToken cancellationToken)
    {
        var signal = new ReplicaApplySignal();
        var wait = signal.WaitAsync(TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        signal.Notify();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>Notifications raised before the wait coalesce into one, and the signal can be raised again afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotificationsCoalesce(CancellationToken cancellationToken)
    {
        var signal = new ReplicaApplySignal();
        signal.Notify();
        signal.Notify();
        signal.Notify();

        _ = await Assert.That(await signal.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That(await signal.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsFalse();

        signal.Notify();
        _ = await Assert.That(await signal.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>With nothing pending the wait ends when the delay elapses on the given clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DelayEndsAnIdleWait(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var signal = new ReplicaApplySignal();
        var wait = signal.WaitAsync(TimeSpan.FromSeconds(1), clock, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>Canceling the token ends the wait by throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsTheWait(CancellationToken cancellationToken)
    {
        var signal = new ReplicaApplySignal();
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = signal.WaitAsync(TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token);

        await canceled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
    }
}
