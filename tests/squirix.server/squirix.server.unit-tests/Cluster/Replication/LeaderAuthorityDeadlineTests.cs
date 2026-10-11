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

/// <summary>A leader that gets no authority within its deadline gives the group up and abstains from campaigning for a while.</summary>
[Immutable]
public sealed class LeaderAuthorityDeadlineTests : ServerUnitTestBase
{
    private static readonly TimeSpan Abstain = Options.ElectionTimeout + Options.MaxJitter + (Options.VoteRpcTimeout * 2);
    private static readonly TimeSpan Slice = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);
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
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.AuthorityTimedOut, 2UL, FailoverDenial.None, TimeSpan.FromSeconds(30)));
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

    /// <summary>After giving up, the node campaigns only once its election timeout plus the abstain time passed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GiveUpAbstainsFromCampaigning(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        _ = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30), cancellationToken);
        var callsBefore = votes.Calls.Length;

        scope.Time.Advance(Options.ElectionTimeout + Abstain - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.None, callsBefore));
        _ = await Assert.That(campaigned).IsEqualTo(new ElectionOutcome(ElectionEvent.Elected, 3UL));
        _ = await Assert.That(votes.Calls.Length).IsGreaterThan(callsBefore);
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
        void Slow()
        {
            scope.Time.Advance(TimeSpan.FromSeconds(6));
            scope.Leadership.DuringNextPromotion = Slow;
        }

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
        _ = await Assert.That((pending, outcome.Event, outcome.Waited)).IsEqualTo((5, ElectionEvent.AuthorityTimedOut, TimeSpan.FromSeconds(30)));
    }

    /// <summary>A give-up whose retirement stays pending is retried like any other pending retirement.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GiveUpRetireIsRetried(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        scope.Leadership.AnswerRetirements(false, false);
        var election = await ElectAsync(scope, cancellationToken);

        var gaveUp = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30), cancellationToken);
        var pending = await election.StepAsync(cancellationToken);
        var retired = await election.StepAsync(cancellationToken);

        _ = await Assert.That((gaveUp.Event, pending.Event, retired.Event)).IsEqualTo((ElectionEvent.AuthorityTimedOut, ElectionEvent.RetirePending, ElectionEvent.SteppedDown));
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False,retire:False,retire:False");
        _ = await Assert.That(scope.State.Role).IsEqualTo(ReplicaGroupRole.Follower);
    }

    private static Task<ElectionOutcome> CampaignAgainAsync(ElectionScope scope, ReplicaGroupElection election, CancellationToken cancellationToken)
    {
        scope.Time.Advance(Options.ElectionTimeout + Abstain);
        return election.StepAsync(cancellationToken);
    }

    private static async Task<ReplicaGroupElection> ElectAsync(ElectionScope scope, CancellationToken cancellationToken)
    {
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        _ = await election.StepAsync(cancellationToken);
        return election;
    }

    private static FollowerLogVoteResult? Grant(VoteCall call) => new(true, string.Empty, call.PreVote ? 0UL : call.Term);

    private static async Task<double> GiveUpAsync(ElectionScope scope, ReplicaGroupElection election, CancellationToken cancellationToken)
    {
        var outcome = await LeadForAsync(scope, election, TimeSpan.FromMinutes(10), cancellationToken);
        return outcome.Event == ElectionEvent.AuthorityTimedOut ? outcome.Waited.TotalSeconds : -1;
    }

    /// <summary>Runs leader ticks for a span of fake time, with one follower answering before each, until the leader gives up.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="election">The driver.</param>
    /// <param name="span">The fake time to run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The outcome of the last tick.</returns>
    private static async Task<ElectionOutcome> LeadForAsync(ElectionScope scope, ReplicaGroupElection election, TimeSpan span, CancellationToken cancellationToken)
    {
        var outcome = new ElectionOutcome(ElectionEvent.None, scope.State.Term);
        var remaining = span;
        while (remaining > TimeSpan.Zero && outcome.Event != ElectionEvent.AuthorityTimedOut)
        {
            var slice = remaining < Slice ? remaining : Slice;
            scope.State.RecordFollowerContact(0, scope.State.Term);
            scope.Time.Advance(slice);
            outcome = await election.StepAsync(cancellationToken);
            remaining -= slice;
        }

        return outcome;
    }
}
