using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;
using static Squirix.Server.UnitTests.Cluster.Replication.LeaderAuthorityDeadlineTests;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A leader with authority whose applied index stands still behind the entries a majority backs gives the group up after the apply timeout.</summary>
[Immutable]
public sealed class LeaderApplyDeadlineTests : ServerUnitTestBase
{
    private const string HeartbeatCall = "heartbeat";

    private static readonly TimeSpan ApplyWindow = TimeSpan.FromSeconds(30);

    /// <summary>The progress of a group whose entries 4 and 5 a majority backs while the applied index stands at 3.</summary>
    private static readonly ReplicaApplyProgress Blocked = new(3UL, 5UL);

    /// <summary>A blocked leader holds the group until the apply timeout, not a tick before, then steps down with authority revoked first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedLeaderHoldsUntilApplyDeadline(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        _ = await LeadForAsync(scope, election, Tick, cancellationToken);
        var early = await LeadForAsync(scope, election, ApplyWindow - Tick, cancellationToken);
        var roleBefore = (scope.State.Role, scope.State.HasAuthority);

        var outcome = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That((early.Event, roleBefore)).IsEqualTo((ElectionEvent.None, (ReplicaGroupRole.Leader, true)));
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.ApplyTimedOut, 2UL, FailoverDenial.None, ApplyWindow));
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
        _ = await Assert.That(scope.Leadership.Calls).DoesNotContain("retire");

        _ = await election.StepAsync(cancellationToken);
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False");
    }

    /// <summary>The timeout is reported and the authority revoked while the retirement would hang behind a stuck apply, and no heartbeat follows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutIsReportedBeforeRetirement(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.RetireHold = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        var outcome = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);
        var heartbeats = CountHeartbeats(scope);

        var retiring = election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var delay = election.NextDelay();

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.ApplyTimedOut);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
        _ = await Assert.That(retiring.IsCompleted).IsFalse();
        _ = await Assert.That((CountHeartbeats(scope), delay)).IsEqualTo((heartbeats, Options.HeartbeatInterval));

        scope.Leadership.RetireHold.SetResult(true);
        _ = await Assert.That((await retiring).Event).IsEqualTo(ElectionEvent.SteppedDown);
        _ = await Assert.That(CountHeartbeats(scope)).IsEqualTo(heartbeats);
    }

    /// <summary>A higher term seen after the apply window expired deposes the leader: no apply timeout, no abstain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedWinsOverExpiredApplyWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        _ = await LeadForAsync(scope, election, Tick, cancellationToken);
        scope.Time.Advance(ApplyWindow);
        scope.State.ObserveHigherTerm(scope.State.Term + 3UL);

        var deposed = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((deposed.Event, deposed.Waited)).IsEqualTo((ElectionEvent.SteppedDown, TimeSpan.Zero));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Authorized);
    }

    /// <summary>Any move of the applied index restarts the window, even while the leader stays behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyProgressRestartsWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        _ = await LeadForAsync(scope, election, TimeSpan.FromSeconds(20), cancellationToken);
        scope.Leadership.Progress = new ReplicaApplyProgress(4UL, 5UL);
        _ = await LeadForAsync(scope, election, Tick, cancellationToken);

        var early = await LeadForAsync(scope, election, ApplyWindow - Tick, cancellationToken);
        var due = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That(due).IsEqualTo(new ElectionOutcome(ElectionEvent.ApplyTimedOut, 2UL, FailoverDenial.None, ApplyWindow));
    }

    /// <summary>Catching up clears the window: a later block starts a new one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaughtUpApplyClearsWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        _ = await LeadForAsync(scope, election, TimeSpan.FromSeconds(25), cancellationToken);
        scope.Leadership.Progress = default;
        _ = await LeadForAsync(scope, election, Tick, cancellationToken);
        scope.Leadership.Progress = Blocked;
        _ = await LeadForAsync(scope, election, Tick, cancellationToken);

        var early = await LeadForAsync(scope, election, ApplyWindow - Tick, cancellationToken);
        var due = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((due.Event, due.Waited)).IsEqualTo((ElectionEvent.ApplyTimedOut, ApplyWindow));
    }

    /// <summary>A leader without authority is bound by the authority timeout alone, whatever its apply progress.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnauthorizedLeaderUsesAuthorityWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        scope.Leadership.Progress = Blocked;
        var election = await ElectAsync(scope, cancellationToken);
        var early = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30) - Tick, cancellationToken);

        var outcome = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.PromotionPending);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL, FailoverDenial.None, TimeSpan.FromSeconds(30)));
    }

    /// <summary>An apply give-up neither counts toward nor lengthens the authority window of the next leadership.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyGiveUpKeepsAuthorityWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        var gaveUp = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);
        var retired = await election.StepAsync(cancellationToken);
        scope.Leadership.Progress = default;
        scope.Leadership.PromotionsPending = true;
        scope.Time.Advance(Options.ElectionTimeout + Abstain);
        var elected = await election.StepAsync(cancellationToken);

        var outcome = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);

        _ = await Assert.That((gaveUp.Event, retired.Event, elected.Event)).IsEqualTo((ElectionEvent.ApplyTimedOut, ElectionEvent.SteppedDown, ElectionEvent.Elected));
        _ = await Assert.That((outcome.Event, outcome.Waited)).IsEqualTo((ElectionEvent.SteppedDown, ApplyWindow));
    }

    /// <summary>A blocked leader whose retirement stays pending does not campaign meanwhile, and abstains from the moment the retirement completes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedLeaderAbstainsWhileRetiring(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        scope.Leadership.AnswerRetirements(false, false);
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Progress = Blocked;
        var gaveUp = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);
        var pending = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(10));
        var stillPending = await election.StepAsync(cancellationToken);
        var callsWhileRetiring = votes.Calls.Length;
        var retired = await election.StepAsync(cancellationToken);

        var atRetire = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Abstain - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        ElectionEvent[] events = [gaveUp.Event, pending.Event, stillPending.Event, retired.Event];
        await SequenceAssert.EqualAsync([ElectionEvent.ApplyTimedOut, ElectionEvent.RetirePending, ElectionEvent.RetirePending, ElectionEvent.SteppedDown], events);
        _ = await Assert.That((atRetire.Event, waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.None, ElectionEvent.None, callsWhileRetiring));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Authorized);
        _ = await Assert.That(votes.Calls.Length).IsGreaterThan(callsWhileWaiting);
    }

    internal static int CountHeartbeats(ElectionScope scope)
    {
        var calls = scope.Leadership.Calls;
        var count = 0;
        var at = calls.IndexOf(HeartbeatCall, StringComparison.Ordinal);
        while (at >= 0)
        {
            count++;
            at = calls.IndexOf(HeartbeatCall, at + HeartbeatCall.Length, StringComparison.Ordinal);
        }

        return count;
    }

    /// <summary>Starts a driver of a three-member group and runs it until its leader-term entry is committed and it has authority.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The driver, leading with authority.</returns>
    /// <exception cref="InvalidOperationException">The leader did not get authority.</exception>
    internal static async Task<ReplicaGroupElection> AuthorizedAsync(ElectionScope scope, CancellationToken cancellationToken)
    {
        scope.Leadership.AnswerPromotions(false);
        var election = await ElectAsync(scope, cancellationToken);
        var authorized = await LeadForAsync(scope, election, Slice, cancellationToken);
        return authorized.Event == ElectionEvent.Authorized
            ? election
            : throw new InvalidOperationException($"The leader must be authorized, but the step was {authorized.Event}.");
    }
}
