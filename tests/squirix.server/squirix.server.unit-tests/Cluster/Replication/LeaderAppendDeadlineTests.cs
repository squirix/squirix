using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;
using static Squirix.Server.UnitTests.Cluster.Replication.LeaderApplyDeadlineTests;
using static Squirix.Server.UnitTests.Cluster.Replication.LeaderAuthorityDeadlineTests;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// A leader with authority whose appends to its own group log keep failing or hang gives the group up after the append timeout, while
/// the followers without it form a majority that can take the group over.
/// </summary>
[Immutable]
public sealed class LeaderAppendDeadlineTests : ServerUnitTestBase
{
    private const int FirstFollowerSlot = 0;
    private const int SecondFollowerSlot = 2;
    private static readonly TimeSpan AppendWindow = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Long = TimeSpan.FromMinutes(10);

    /// <summary>The progress of a leader whose log refuses every append after an earlier failure.</summary>
    private static readonly ReplicaAppendProgress Refusing = new(0UL, 3UL, true);

    /// <summary>A failing leader holds the group until the append timeout, not a tick before, then steps down with authority revoked first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailingLeaderHoldsUntilAppendDeadline(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        var early = await RunAsync(scope, election, AppendWindow - Tick, true, cancellationToken);
        var roleBefore = (scope.State.Role, scope.State.HasAuthority);

        var outcome = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That((early.Event, roleBefore)).IsEqualTo((ElectionEvent.None, (ReplicaGroupRole.Leader, true)));
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.AppendTimedOut, 2UL, FailoverDenial.None, AppendWindow));
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
        _ = await Assert.That(scope.Leadership.Calls).DoesNotContain("retire");

        _ = await election.StepAsync(cancellationToken);
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False");
    }

    /// <summary>A successful append restarts the window, even while the leader stays blocked afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendRestartsWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        _ = await RunAsync(scope, election, TimeSpan.FromSeconds(20), true, cancellationToken);
        scope.Leadership.Appends = Refusing with { Appended = 1UL };
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);

        var early = await RunAsync(scope, election, AppendWindow - Tick, true, cancellationToken);
        var due = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((due.Event, due.Waited)).IsEqualTo((ElectionEvent.AppendTimedOut, AppendWindow));
    }

    /// <summary>A window that expired holds the group while no new failure appears and nothing is blocked: the stall is not visible.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoEvidenceAfterDeadlineHolds(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = new ReplicaAppendProgress(0UL, 3UL, false);
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);

        var outcome = await RunAsync(scope, election, Long, true, cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, true));
    }

    /// <summary>A failure after the window expired gives the group up: a failure, an idle period, and another failure end the leadership.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailureAfterDeadlineGivesUp(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = new ReplicaAppendProgress(0UL, 3UL, false);
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        _ = await RunAsync(scope, election, Long, true, cancellationToken);
        scope.Leadership.Appends = new ReplicaAppendProgress(0UL, 4UL, false);

        var outcome = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.AppendTimedOut);
        _ = await Assert.That(outcome.Waited).IsGreaterThanOrEqualTo(Long);
    }

    /// <summary>An append that never returns, with no failure counted, gives the group up after the append timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedAppendTimesOut(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = new ReplicaAppendProgress(0UL, 0UL, true);
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        var early = await RunAsync(scope, election, AppendWindow - Tick, true, cancellationToken);

        var due = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((due.Event, due.Waited)).IsEqualTo((ElectionEvent.AppendTimedOut, AppendWindow));
    }

    /// <summary>The first observation after the leader got authority only records the counters: failures from before it start no window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BaselineAfterAuthorityStartsNoWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await AuthorizedAsync(scope, cancellationToken);
        scope.Leadership.Appends = new ReplicaAppendProgress(0UL, 5UL, false);

        var outcome = await RunAsync(scope, election, Long, true, cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, true));
    }

    /// <summary>
    /// Without a majority of followers that answer, nobody could take the group, so the leader keeps it; it gives the group up at the first
    /// tick after the contact is back.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFollowerMajorityHolds(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        var held = await RunAsync(scope, election, Long, false, cancellationToken);
        var roleHeld = (scope.State.Role, scope.State.HasAuthority);

        var outcome = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That((held.Event, roleHeld)).IsEqualTo((ElectionEvent.None, (ReplicaGroupRole.Leader, true)));
        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.AppendTimedOut);
    }

    /// <summary>A higher term seen after the append window expired deposes the leader: no append timeout, no abstain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedWinsOverAppendWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        scope.Time.Advance(AppendWindow);
        scope.State.ObserveHigherTerm(scope.State.Term + 3UL);

        var deposed = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((deposed.Event, deposed.Waited)).IsEqualTo((ElectionEvent.SteppedDown, TimeSpan.Zero));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Authorized);
    }

    /// <summary>A leader without authority is bound by the authority timeout alone, whatever its appends do.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnauthorizedLeaderUsesAuthorityWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        scope.Leadership.Appends = Refusing;
        var election = await ElectAsync(scope, cancellationToken);
        var early = await RunAsync(scope, election, TimeSpan.FromSeconds(30) - Tick, true, cancellationToken);

        var outcome = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.PromotionPending);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL, FailoverDenial.None, TimeSpan.FromSeconds(30)));
    }

    /// <summary>An append give-up neither counts toward nor lengthens the authority window of the next leadership.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendGiveUpKeepsAuthorityWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        var gaveUp = await RunAsync(scope, election, Long, true, cancellationToken);
        var retired = await election.StepAsync(cancellationToken);
        scope.Leadership.Appends = default;
        scope.Leadership.PromotionsPending = true;
        scope.Time.Advance(Options.ElectionTimeout + Abstain);
        var elected = await election.StepAsync(cancellationToken);

        var outcome = await RunAsync(scope, election, Long, true, cancellationToken);

        _ = await Assert.That((gaveUp.Event, retired.Event, elected.Event)).IsEqualTo((ElectionEvent.AppendTimedOut, ElectionEvent.SteppedDown, ElectionEvent.Elected));
        _ = await Assert.That((outcome.Event, outcome.Waited)).IsEqualTo((ElectionEvent.SteppedDown, AppendWindow));
    }

    /// <summary>The timeout is reported and the authority revoked while the retirement would hang behind a stuck append, and no heartbeat follows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendTimeoutIsReportedBeforeRetirement(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.RetireHold = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Appends = Refusing;
        var outcome = await RunAsync(scope, election, Long, true, cancellationToken);
        var heartbeats = CountHeartbeats(scope);

        var retiring = election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var delay = election.NextDelay();

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.AppendTimedOut);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
        _ = await Assert.That(retiring.IsCompleted).IsFalse();
        _ = await Assert.That((CountHeartbeats(scope), delay)).IsEqualTo((heartbeats, Options.HeartbeatInterval));

        scope.Leadership.RetireHold.SetResult(true);
        _ = await Assert.That((await retiring).Event).IsEqualTo(ElectionEvent.SteppedDown);
        _ = await Assert.That(CountHeartbeats(scope)).IsEqualTo(heartbeats);
    }

    /// <summary>A leader that can neither apply nor append gives up on the apply timeout, which is judged first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyTimeoutWinsOverAppendTimeout(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await BaselinedAsync(scope, cancellationToken);
        scope.Leadership.Progress = new ReplicaApplyProgress(3UL, 5UL);
        scope.Leadership.Appends = Refusing;
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        var early = await RunAsync(scope, election, AppendWindow - Tick, true, cancellationToken);

        var due = await RunAsync(scope, election, Tick, true, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((due.Event, due.Waited)).IsEqualTo((ElectionEvent.ApplyTimedOut, AppendWindow));
    }

    /// <summary>Starts a driver, runs it until it has authority, and lets one healthy tick record the append counters.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The driver, leading with authority.</returns>
    private static async Task<ReplicaGroupElection> BaselinedAsync(ElectionScope scope, CancellationToken cancellationToken)
    {
        var election = await AuthorizedAsync(scope, cancellationToken);
        _ = await RunAsync(scope, election, Tick, true, cancellationToken);
        return election;
    }

    /// <summary>Runs leader ticks for a span of fake time, with the followers answering before each, until the leader gives up.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="election">The driver.</param>
    /// <param name="span">The fake time to run.</param>
    /// <param name="bothFollowers">Whether the second follower answers as well; otherwise only the first does.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The outcome of the last tick; the run ends at a step-down or a timeout.</returns>
    private static async Task<ElectionOutcome> RunAsync(ElectionScope scope, ReplicaGroupElection election, TimeSpan span, bool bothFollowers, CancellationToken cancellationToken)
    {
        var outcome = new ElectionOutcome(ElectionEvent.None, scope.State.Term);
        var remaining = span;
        while (remaining > TimeSpan.Zero && outcome.Event is not (ElectionEvent.SteppedDown or ElectionEvent.ApplyTimedOut or ElectionEvent.AppendTimedOut))
        {
            var slice = remaining < Slice ? remaining : Slice;
            scope.State.RecordFollowerContact(FirstFollowerSlot, scope.State.Term);
            if (bothFollowers)
                scope.State.RecordFollowerContact(SecondFollowerSlot, scope.State.Term);

            scope.Time.Advance(slice);
            outcome = await election.StepAsync(cancellationToken);
            remaining -= slice;
        }

        return outcome;
    }
}
