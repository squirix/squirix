using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The election driver of one group: when it campaigns, how it counts, how it leads, and how it steps down.</summary>
[Immutable]
public sealed class ReplicaGroupElectionTests : ServerUnitTestBase
{
    private static readonly string[] Three = ["n2", "n1", "n3"];

    /// <summary>A follower campaigns only once the leader stayed silent for the election timeout, and the first election is for term two.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CampaignWaitsForSilentLeader(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromMilliseconds(400));
        scope.State.ObserveLeaderContact("n2", 0UL);
        scope.Time.Advance(TimeSpan.FromMilliseconds(400));

        var waiting = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromMilliseconds(100));
        var elected = await election.StepAsync(cancellationToken);

        _ = await Assert.That(waiting.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That(elected).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(CountCalls(votes, static call => call.Term != 2UL)).IsEqualTo(0);
        var status = await scope.Log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((2UL, "n1"));
    }

    /// <summary>A refused pre-vote changes no term anywhere and never reaches the vote round.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteRefusalKeepsTerm(CancellationToken cancellationToken)
    {
        var expectations = new IReplicaVoteGatewayCreateExpectations();
        _ = expectations.Setups.PreVoteAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
                        .ReturnValue(Task.FromResult(new FollowerLogVoteResult(false, RefusalCodes.LeaderContact, 0UL)));
        await using var scope = await OpenAsync("n2", 3, expectations.Instance());
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.PreVoteLost, 0UL));
        var status = await scope.Log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, scope.State.Role)).IsEqualTo((0UL, string.Empty, ReplicaGroupRole.Follower));
    }

    /// <summary>Two of five voters unreachable: the three grants with the own vote still elect.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThreeOfFiveElect(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(static call => string.Equals(call.NodeId, "n4", StringComparison.Ordinal) || string.Equals(call.NodeId, "n5", StringComparison.Ordinal) ? null : Grant(call));
        await using var scope = await OpenAsync("n2", 5, votes);
        var election = CreateElection(scope, ["n2", "n1", "n3", "n4", "n5"]);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo("promote:2");
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, true));
    }

    /// <summary>One grant of five is no majority: the pre-vote round ends it before any term changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MinorityOfGrantsLoses(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(static call => string.Equals(call.NodeId, "n3", StringComparison.Ordinal) ? Grant(call) : new FollowerLogVoteResult(false, RefusalCodes.StaleLog, 0UL));
        await using var scope = await OpenAsync("n2", 5, votes);
        var election = CreateElection(scope, ["n2", "n1", "n3", "n4", "n5"]);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.PreVoteLost);
        _ = await Assert.That(CountCalls(votes, static call => !call.PreVote)).IsEqualTo(0);
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(0UL);
    }

    /// <summary>A vote refused with a higher term is made durable and followed; the candidate never leads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherVoteTermIsFollowed(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(static call => call.PreVote ? Grant(call) : new FollowerLogVoteResult(false, RefusalCodes.StaleTerm, 6UL));
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.TermObserved, 6UL));
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(6UL);
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo(string.Empty);
    }

    /// <summary>A split vote loses the term and re-arms the election with a different jitter each time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SplitVoteRearmsWithFreshJitter(CancellationToken cancellationToken)
    {
        var options = new ElectionTimerOptions { ElectionTimeout = Options.ElectionTimeout, MaxJitter = TimeSpan.FromMilliseconds(500), JitterSeed = 11UL };
        var votes = new ScriptedVotes(static call => call.PreVote ? Grant(call) : new FollowerLogVoteResult(false, RefusalCodes.AlreadyVoted, call.Term));
        await using var scope = await OpenAsync("n2", 3, votes, options);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(1));

        var first = await election.StepAsync(cancellationToken);
        var firstDelay = election.NextDelay();
        scope.Time.Advance(TimeSpan.FromSeconds(1));
        var second = await election.StepAsync(cancellationToken);

        _ = await Assert.That((first, second)).IsEqualTo((new ElectionOutcome(ElectionEvent.VoteLost, 2UL), new ElectionOutcome(ElectionEvent.VoteLost, 3UL)));
        _ = await Assert.That(firstDelay).IsNotEqualTo(election.NextDelay());
        _ = await Assert.That(firstDelay).IsGreaterThanOrEqualTo(Options.ElectionTimeout);
    }

    /// <summary>A won term whose leader-term entry is not committed yet holds no authority; the promotion is retried in the same term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingPromotionRetriesWithoutAuthority(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.AnswerPromotions(false);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var elected = await election.StepAsync(cancellationToken);
        var authorityBefore = scope.State.HasAuthority;
        var retried = await election.StepAsync(cancellationToken);

        _ = await Assert.That((elected.Event, authorityBefore)).IsEqualTo((ElectionEvent.Elected, false));
        _ = await Assert.That(retried).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo("promote:2,heartbeat,promote:2");
    }

    /// <summary>A leader no majority answered for an election timeout steps down, revoking its authority before it retires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentMajorityDeposesLeader(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await ElectAsync(scope, cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL));
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False");
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Follower, false));
    }

    /// <summary>A first promotion slower than the election timeout does not depose the new leader before its first heartbeat.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowFirstPromotionKeepsQuorumGrace(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.AnswerPromotions(false);
        scope.Leadership.DuringNextPromotion = () => scope.Time.Advance(Options.ElectionTimeout * 2);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var elected = await election.StepAsync(cancellationToken);
        var retried = await election.StepAsync(cancellationToken);

        _ = await Assert.That(elected).IsEqualTo(new ElectionOutcome(ElectionEvent.Elected, 2UL));
        _ = await Assert.That(retried).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo("promote:2,heartbeat,promote:2");
    }

    /// <summary>A retried start that holds the driver longer than the election timeout does not depose the leader before its next heartbeat.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowRetriedStartKeepsQuorumGrace(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.AnswerPromotions(false, false);
        var election = await ElectAsync(scope, cancellationToken);
        scope.Leadership.DuringNextPromotion = () => scope.Time.Advance(Options.ElectionTimeout * 2);

        var retried = await election.StepAsync(cancellationToken);
        var authorized = await election.StepAsync(cancellationToken);

        _ = await Assert.That((retried.Event, authorized)).IsEqualTo((ElectionEvent.PromotionPending, new ElectionOutcome(ElectionEvent.Authorized, 2UL)));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo("promote:2,heartbeat,promote:2,heartbeat,promote:2");
    }

    /// <summary>Quick promotions that stay pending restart no grace: a leader no follower answers steps down after one timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QuickPendingPromotionStepsDown(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.AnswerPromotions(false, false, false);
        var election = await ElectAsync(scope, cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout / 2);
        var pending = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout / 2);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That((pending.Event, outcome)).IsEqualTo((ElectionEvent.PromotionPending, new ElectionOutcome(ElectionEvent.SteppedDown, 2UL)));
    }

    /// <summary>A leader authorized by its first promotion keeps the grace of its election: a silent majority still deposes it after one timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthorizedLeaderKeepsElectionGrace(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.DuringNextPromotion = () => scope.Time.Advance(Options.ElectionTimeout);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var elected = await election.StepAsync(cancellationToken);
        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(elected).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 2UL));
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL));
    }

    /// <summary>A follower that keeps answering keeps the leader; a higher term then deposes it, and the term is durable before it retires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermDeposesLeader(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await ElectAsync(scope, cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        scope.State.RecordFollowerReply(0, new FollowerLogAppendResult(true, string.Empty, 2UL, 0UL));
        var kept = await election.StepAsync(cancellationToken);

        scope.State.RecordFollowerReply(2, new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, 7UL, 0UL));
        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(kept.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 7UL));
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(7UL);
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False");
    }

    /// <summary>A retirement that cannot finish is retried on the next steps, before any new campaign.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingRetireIsRetried(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        var election = await ElectAsync(scope, cancellationToken);
        scope.Leadership.AnswerRetirements(false);
        scope.State.ObserveHigherTerm(4UL);

        var pending = await election.StepAsync(cancellationToken);
        scope.Time.Advance(TimeSpan.FromSeconds(5));
        var retired = await election.StepAsync(cancellationToken);

        _ = await Assert.That((pending.Event, retired.Event)).IsEqualTo((ElectionEvent.RetirePending, ElectionEvent.SteppedDown));
        _ = await Assert.That(scope.Leadership.Calls).EndsWith("retire:False,retire:False");
    }

    /// <summary>The own group still at the provisional term is led at term one without any vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupLeadsProvisionalTerm(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n1", 3, votes);
        var election = CreateElection(scope, ["n1", "n2", "n3"]);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 1UL));
        _ = await Assert.That(votes.Calls.Length).IsEqualTo(0);
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(1UL);
    }

    /// <summary>The own group past the provisional term is followed like any other group until it wins an election.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupPastProvisionalTermFollows(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n1", 3, new ScriptedVotes(Grant));
        _ = await scope.Log.ObserveTermAsync(2UL, cancellationToken);
        var election = CreateElection(scope, ["n1", "n2", "n3"]);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.None, 2UL));
        _ = await Assert.That((scope.State.Role, scope.State.IsElectionDriven)).IsEqualTo((ReplicaGroupRole.Follower, true));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo(string.Empty);
    }

    private static int CountCalls(ScriptedVotes votes, Func<VoteCall, bool> match)
    {
        var count = 0;
        foreach (var call in votes.Calls)
        {
            if (match(call))
                count++;
        }

        return count;
    }

    private static FollowerLogVoteResult? Grant(VoteCall call) => new(true, string.Empty, call.PreVote ? 0UL : call.Term);

    private static async Task<ReplicaGroupElection> ElectAsync(ElectionScope scope, CancellationToken cancellationToken)
    {
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        _ = await election.StepAsync(cancellationToken);
        return election;
    }
}
