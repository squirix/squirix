using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A leader that gets no authority within its window after the first failed promotion gives the group up.</summary>
[Immutable]
public sealed class LeaderAuthorityDeadlineTests : ServerUnitTestBase
{
    /// <summary>The time a node does not campaign after it gave a group up, with the timing of the driver tests.</summary>
    internal static readonly TimeSpan Abstain = Options.ElectionTimeout + Options.MaxJitter + (Options.VoteRpcTimeout * 2);

    /// <summary>The span of one leader tick of <see cref="LeadForAsync" />.</summary>
    internal static readonly TimeSpan Slice = TimeSpan.FromMilliseconds(400);

    /// <summary>The smallest step of fake time.</summary>
    internal static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);

    private static readonly string[] Three = ["n2", "n1", "n3"];

    /// <summary>A leader whose followers keep answering but whose entry never commits gives up at the deadline, not a tick before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnauthorizedLeaderGivesUp(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        var early = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30) - Tick, cancellationToken);
        var roleBefore = scope.State.Role;

        var outcome = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That((early.Event, roleBefore)).IsEqualTo((ElectionEvent.PromotionPending, ReplicaGroupRole.Leader));
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL, FailoverDenial.None, TimeSpan.FromSeconds(30)));
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False");
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
    }

    /// <summary>A leader with authority is never timed out, however long it leads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthorizedLeaderIsNeverTimedOut(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.AnswerPromotions(false);
        var election = await ElectAsync(scope, cancellationToken);
        var authorized = await LeadForAsync(scope, election, Slice, cancellationToken);

        var outcome = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);

        _ = await Assert.That(authorized.Event).IsEqualTo(ElectionEvent.Authorized);
        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, true));
    }

    /// <summary>A first promotion that itself takes longer than the window does not make the leader give up on the next tick.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowFirstPromotionIsNotCounted(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        scope.Leadership.DuringNextPromotion = () => scope.Time.Advance(TimeSpan.FromSeconds(45));
        var election = await ElectAsync(scope, cancellationToken);

        var next = await LeadForAsync(scope, election, Tick, cancellationToken);
        var early = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30) - (Tick * 2), cancellationToken);
        var due = await LeadForAsync(scope, election, Tick * 2, cancellationToken);

        _ = await Assert.That((next.Event, early.Event)).IsEqualTo((ElectionEvent.PromotionPending, ElectionEvent.PromotionPending));
        _ = await Assert.That((due.Event, due.Waited >= TimeSpan.FromSeconds(30))).IsEqualTo((ElectionEvent.SteppedDown, true));
    }

    /// <summary>A leader past its window that also lost its majority steps down as an ordinary step-down and counts no give-up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentMajorityIsNoGiveUp(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(31));

        var stepped = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var elected = await election.StepAsync(cancellationToken);
        var wait = await GiveUpAsync(scope, election, cancellationToken);

        _ = await Assert.That((stepped.Event, stepped.Waited)).IsEqualTo((ElectionEvent.SteppedDown, TimeSpan.Zero));
        _ = await Assert.That((elected.Event, wait)).IsEqualTo((ElectionEvent.Elected, 30d));
    }

    /// <summary>A leadership deposed without authority between two give-ups neither restarts nor lengthens the window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedLeaderKeepsTheWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        var first = await GiveUpAsync(scope, election, cancellationToken);
        var second = await CampaignAgainAsync(scope, election, cancellationToken);
        scope.State.ObserveHigherTerm(scope.State.Term + 3UL);
        var deposed = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var third = await election.StepAsync(cancellationToken);
        var last = await GiveUpAsync(scope, election, cancellationToken);

        _ = await Assert.That((second.Event, deposed.Event, deposed.Waited, third.Event)).IsEqualTo((ElectionEvent.Elected, ElectionEvent.SteppedDown, TimeSpan.Zero, ElectionEvent.Elected));
        _ = await Assert.That((first, last)).IsEqualTo((30d, 60d));
    }

    /// <summary>Each give-up doubles the next window up to eight times the timeout, and authority restarts it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GiveUpsDoubleTheWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        var waits = new double[6];
        for (var i = 0; i < 5; i++)
        {
            waits[i] = await GiveUpAsync(scope, election, cancellationToken);
            if (i < 4)
                _ = await CampaignAgainAsync(scope, election, cancellationToken);
        }

        scope.Leadership.PromotionsPending = false;
        var authorized = await CampaignAgainAsync(scope, election, cancellationToken);
        scope.Leadership.PromotionsPending = true;
        scope.State.ObserveHigherTerm(scope.State.Term + 3UL);
        var deposed = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var elected = await election.StepAsync(cancellationToken);
        waits[5] = await GiveUpAsync(scope, election, cancellationToken);

        _ = await Assert.That((authorized.Event, deposed.Event, elected.Event)).IsEqualTo((ElectionEvent.Authorized, ElectionEvent.SteppedDown, ElectionEvent.Elected));
        _ = await Assert.That((waits[0], waits[1], waits[2], waits[3], waits[4], waits[5])).IsEqualTo((30d, 60d, 120d, 240d, 240d, 30d));
    }

    /// <summary>A start whose promotion takes longer than a heartbeat on every retry still times out.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowStartStillGivesUp(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;

        scope.Leadership.DuringNextPromotion = Slow;
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var elected = await election.StepAsync(cancellationToken);
        var pending = 0;
        ElectionOutcome outcome;
        do
        {
            outcome = await election.StepAsync(cancellationToken);
            pending++;
        }
        while (outcome.Event == ElectionEvent.PromotionPending);

        _ = await Assert.That(elected.Event).IsEqualTo(ElectionEvent.Elected);
        _ = await Assert.That((pending, outcome.Event, outcome.Waited.TotalSeconds)).IsEqualTo((6, ElectionEvent.SteppedDown, 30d));
        return;

        void Slow()
        {
            scope.Time.Advance(TimeSpan.FromSeconds(6));
            scope.Leadership.DuringNextPromotion = Slow;
        }
    }

    /// <summary>A step-down on a higher term or a silent majority reports no wait for authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OrdinaryStepDownReportsNoWait(CancellationToken cancellationToken)
    {
        await using var higher = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var higherElection = await ElectAsync(higher, cancellationToken);
        higher.State.ObserveHigherTerm(7UL);
        var deposed = await higherElection.StepAsync(cancellationToken);
        await using var silent = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var silentElection = await ElectAsync(silent, cancellationToken);
        silent.Time.Advance(Options.ElectionTimeout);
        var abandoned = await silentElection.StepAsync(cancellationToken);

        _ = await Assert.That((deposed.Event, deposed.Waited.TotalSeconds)).IsEqualTo((ElectionEvent.SteppedDown, 0d));
        _ = await Assert.That((abandoned.Event, abandoned.Waited.TotalSeconds)).IsEqualTo((ElectionEvent.SteppedDown, 0d));
    }

    /// <summary>Starts a driver of a three-member group and runs it until it won the election after its full timeout.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The driver, leading.</returns>
    internal static async Task<ReplicaGroupElection> ElectAsync(ElectionScope scope, CancellationToken cancellationToken)
    {
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(scope.State.Options.ElectionTimeout + scope.State.Options.MaxJitter);
        _ = await election.StepAsync(cancellationToken);
        return election;
    }

    /// <summary>Answers every vote call with a grant.</summary>
    /// <param name="call">The vote call.</param>
    /// <returns>The grant.</returns>
    internal static FollowerLogVoteResult? Grant(VoteCall call) => new(true, string.Empty, call.PreVote ? 0UL : call.Term);

    /// <summary>Runs leader ticks for a span of fake time, with one follower answering before each, until the leader gives up.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="election">The driver.</param>
    /// <param name="span">The fake time to run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The outcome of the last tick.</returns>
    internal static async Task<ElectionOutcome> LeadForAsync(ElectionScope scope, ReplicaGroupElection election, TimeSpan span, CancellationToken cancellationToken)
    {
        var outcome = new ElectionOutcome(ElectionEvent.None, scope.State.Term);
        var remaining = span;
        while (remaining > TimeSpan.Zero && outcome.Event != ElectionEvent.SteppedDown)
        {
            var slice = remaining < Slice ? remaining : Slice;
            scope.State.RecordFollowerContact(0, scope.State.Term);
            scope.Time.Advance(slice);
            outcome = await election.StepAsync(cancellationToken);
            remaining -= slice;
        }

        return outcome;
    }

    private static Task<ElectionOutcome> CampaignAgainAsync(ElectionScope scope, ReplicaGroupElection election, CancellationToken cancellationToken)
    {
        scope.Time.Advance(Options.ElectionTimeout + Abstain);
        return election.StepAsync(cancellationToken);
    }

    private static async Task<double> GiveUpAsync(ElectionScope scope, ReplicaGroupElection election, CancellationToken cancellationToken)
    {
        var outcome = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);
        return outcome.Event == ElectionEvent.SteppedDown ? outcome.Waited.TotalSeconds : -1;
    }
}
