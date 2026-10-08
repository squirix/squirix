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

/// <summary>The broadcast signal that tells waiters the leader route of a group may have changed.</summary>
[Immutable]
public sealed class ReplicaRouteSignalTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly ElectionTimerOptions Options = new() { JitterSeed = 7UL };

    /// <summary>One publication wakes every waiter, and a wait from an older version ends at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishWakesEveryWaiter(CancellationToken cancellationToken)
    {
        var signal = new ReplicaRouteSignal();
        var time = new FakeTimeProvider();
        var seen = signal.Version;
        var first = signal.WaitAsync(seen, TimeSpan.FromHours(1), time, cancellationToken);
        var second = signal.WaitAsync(seen, TimeSpan.FromHours(1), time, cancellationToken);
        var pending = first.IsCompleted || second.IsCompleted;

        signal.Publish();

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await first.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That(await second.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        var late = signal.WaitAsync(seen, TimeSpan.FromHours(1), time, cancellationToken);
        _ = await Assert.That((late.IsCompleted, await late)).IsEqualTo((true, true));
    }

    /// <summary>Without a publication the wait ends as a timeout when the delay elapses on the given clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutReturnsFalse(CancellationToken cancellationToken)
    {
        var signal = new ReplicaRouteSignal();
        var time = new FakeTimeProvider();
        var wait = signal.WaitAsync(signal.Version, TimeSpan.FromSeconds(2), time, cancellationToken);
        time.Advance(TimeSpan.FromMilliseconds(1999));
        var early = wait.IsCompleted;

        time.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(early).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>A delay longer than a timer accepts is clamped instead of throwing, and an infinite one waits for a publication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongDelayIsClamped(CancellationToken cancellationToken)
    {
        var signal = new ReplicaRouteSignal();
        var time = new FakeTimeProvider();
        var seen = signal.Version;
        var clamped = signal.WaitAsync(seen, TimeSpan.FromDays(100), time, cancellationToken);
        var infinite = signal.WaitAsync(seen, Timeout.InfiniteTimeSpan, time, cancellationToken);
        time.Advance(TimeSpan.FromDays(40));
        var pending = clamped.IsCompleted || infinite.IsCompleted;
        time.Advance(ReplicaRouteSignal.MaxDelay);
        var timedOut = await clamped.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var infinitePending = infinite.IsCompleted;

        signal.Publish();

        _ = await Assert.That((pending, timedOut, infinitePending)).IsEqualTo((false, false, false));
        _ = await Assert.That(await infinite.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>A canceled wait throws instead of reporting a timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledWaitThrows(CancellationToken cancellationToken)
    {
        var signal = new ReplicaRouteSignal();
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = signal.WaitAsync(signal.Version, TimeSpan.FromHours(1), new FakeTimeProvider(), cancel.Token);

        await cancel.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait);
    }

    /// <summary>The election state publishes every role change, a granted or revoked authority, and a new known leader, but not a repeated contact.</summary>
    [Test]
    public async Task StatePublishesRouteChanges()
    {
        var state = new ReplicaGroupState(3, Options, new FakeTimeProvider());
        var start = state.RouteChanged.Version;
        state.ObserveLeaderContact("n2", 4UL);
        var contact = state.RouteChanged.Version;
        state.ObserveLeaderContact("n2", 4UL);
        var repeated = state.RouteChanged.Version;
        state.SetElectionDriven(true);
        state.BecomePreCandidate();
        state.BecomeCandidate(5UL);
        var campaign = state.RouteChanged.Version;
        _ = state.BecomeLeader(5UL);
        _ = state.GrantAuthority(5UL);
        var granted = state.RouteChanged.Version;
        state.ObserveHigherTerm(6UL);

        _ = await Assert.That(contact).IsEqualTo(start + 1);
        _ = await Assert.That(repeated).IsEqualTo(contact);
        _ = await Assert.That(campaign).IsEqualTo(contact + 2);
        _ = await Assert.That(granted).IsEqualTo(campaign + 2);
        _ = await Assert.That(state.RouteChanged.Version).IsEqualTo(granted + 1);
    }
}
