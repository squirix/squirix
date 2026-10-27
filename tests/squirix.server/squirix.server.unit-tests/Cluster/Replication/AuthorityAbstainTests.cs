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
using static Squirix.Server.UnitTests.Cluster.Replication.LeaderAuthorityDeadlineTests;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A node that gave a group up does not campaign before a point in time that starts when its retirement completed.</summary>
[Immutable]
public sealed class AuthorityAbstainTests : ServerUnitTestBase
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(30);

    /// <summary>After giving up, the node starts no pre-vote and no vote before the abstain point, and campaigns at it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GiveUpAbstainsFromCampaigning(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        _ = await LeadForAsync(scope, election, Window, cancellationToken);
        var callsBefore = votes.Calls.Length;
        var delay = election.NextDelay();

        scope.Time.Advance(Abstain - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That(delay).IsEqualTo(Abstain);
        _ = await Assert.That((waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.None, callsBefore));
        _ = await Assert.That(campaigned).IsEqualTo(new ElectionOutcome(ElectionEvent.Elected, 3UL));
        _ = await Assert.That(votes.Calls.Length).IsGreaterThan(callsBefore);
    }

    /// <summary>The abstain includes the largest jitter of the election timer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbstainIncludesJitter(CancellationToken cancellationToken)
    {
        var jitter = TimeSpan.FromMilliseconds(200);
        var options = new ElectionTimerOptions
        {
            ElectionTimeout = Options.ElectionTimeout,
            HeartbeatInterval = Options.HeartbeatInterval,
            MaxJitter = jitter,
            VoteRpcTimeout = Options.VoteRpcTimeout,
            JitterSeed = Options.JitterSeed,
        };
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes, options);
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        _ = await LeadForAsync(scope, election, Window, cancellationToken);
        var callsBefore = votes.Calls.Length;

        scope.Time.Advance(Abstain + jitter - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.None, callsBefore));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Elected);
    }

    /// <summary>A node that gave up and then followed another leader has a normal election timeout in a later failover.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoAbstainAfterFollowing(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        _ = await LeadForAsync(scope, election, Window, cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(10));
        scope.State.ObserveHigherTerm(scope.State.Term + 3UL);
        var followed = await election.StepAsync(cancellationToken);
        var callsBefore = votes.Calls.Length;

        scope.Time.Advance(Options.ElectionTimeout - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((followed.Event, waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.TermObserved, ElectionEvent.None, callsBefore));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Elected);
    }

    /// <summary>A give-up whose retirement stays pending is retried, and the abstain starts when the retirement completes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbstainStartsWhenRetireCompletes(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        scope.Leadership.PromotionsPending = true;
        scope.Leadership.AnswerRetirements(false, false);
        var election = await ElectAsync(scope, cancellationToken);
        var gaveUp = await LeadForAsync(scope, election, Window, cancellationToken);
        var pending = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(10));
        var retired = await election.StepAsync(cancellationToken);
        var callsBefore = votes.Calls.Length;

        var atRetire = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Abstain - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((gaveUp.Event, pending.Event, retired.Event)).IsEqualTo((ElectionEvent.SteppedDown, ElectionEvent.RetirePending, ElectionEvent.SteppedDown));
        _ = await Assert.That((gaveUp.Waited, retired.Waited)).IsEqualTo((Window, TimeSpan.Zero));
        _ = await Assert.That(scope.Leadership.Calls).Contains("retire:False,retire:False,retire:False");
        _ = await Assert.That((atRetire.Event, waiting.Event, callsWhileWaiting)).IsEqualTo((ElectionEvent.None, ElectionEvent.None, callsBefore));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Elected);
    }
}
