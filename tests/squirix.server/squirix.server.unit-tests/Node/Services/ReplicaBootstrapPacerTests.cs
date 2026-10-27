using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The pacer of the first start runs the next iteration on a call of the member waited for, and paces iterations that find nothing new.</summary>
[Immutable]
public sealed class ReplicaBootstrapPacerTests : ServerUnitTestBase
{
    private static readonly TimeSpan Floor = ReplicaGroupBootstrapService.InitialDelay;
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);
    private static readonly ReplicaBootstrapOutcome WaitingForN3 = new(ReplicaBootstrapResult.WaitingForMember, "n3");

    /// <summary>The first call of the member waited for ends the wait at once, with no time passing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallOfAwaitedMemberEndsWaitAtOnce(CancellationToken cancellationToken)
    {
        var (pacer, contact, clock) = Create();
        pacer.Begin();
        contact.Notify("n3");

        var wait = pacer.WaitAsync(WaitingForN3, cancellationToken);

        _ = await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>A call of another peer does not end the wait; only the backoff does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallOfOtherPeerDoesNotEndWait(CancellationToken cancellationToken)
    {
        var (pacer, contact, clock) = Create();
        pacer.Begin();
        contact.Notify("n2");

        var wait = pacer.WaitAsync(WaitingForN3, cancellationToken);
        clock.Advance(Floor - TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(wait.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        _ = await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    /// <summary>A call that arrived before the iteration began is forgotten: the iteration read the state of the peer itself.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallBeforeIterationIsForgotten(CancellationToken cancellationToken)
    {
        var (pacer, contact, clock) = Create();
        contact.Notify("n3");
        pacer.Begin();

        var wait = pacer.WaitAsync(WaitingForN3, cancellationToken);
        clock.Advance(Floor - TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(wait.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        _ = await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    /// <summary>After an iteration that found the same outcome, a call of the member waits for the initial delay since that iteration started.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedOutcomeWaitsForTheFloor(CancellationToken cancellationToken)
    {
        var (pacer, contact, clock) = Create();
        pacer.Begin();
        contact.Notify("n3");
        _ = await pacer.WaitAsync(WaitingForN3, cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        pacer.Begin();
        contact.Notify("n3");

        var wait = pacer.WaitAsync(WaitingForN3, cancellationToken);
        clock.Advance(Floor - TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(wait.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        _ = await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    /// <summary>A changed outcome is answered at once even right after another call of the member: the member is not the same obstacle any more.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChangedOutcomeIsNotPaced(CancellationToken cancellationToken)
    {
        var (pacer, contact, clock) = Create();
        pacer.Begin();
        contact.Notify("n3");
        _ = await pacer.WaitAsync(WaitingForN3, cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        pacer.Begin();
        contact.Notify("n2");

        var wait = pacer.WaitAsync(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, "n2"), cancellationToken);

        _ = await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Canceling the token ends the wait with the canceled result, without an exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsWaitAsCanceled(CancellationToken cancellationToken)
    {
        var (pacer, _, _) = Create();
        pacer.Begin();
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = pacer.WaitAsync(WaitingForN3, canceled.Token);

        await canceled.CancelAsync();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Canceled);
    }

    private static (ReplicaBootstrapPacer Pacer, ReplicaPeerContact Contact, FakeTimeProvider Clock) Create()
    {
        var contact = new ReplicaPeerContact();
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        return (new ReplicaBootstrapPacer(contact, clock), contact, clock);
    }
}
