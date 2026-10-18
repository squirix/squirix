using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ElectionTestDoubles;
using static Squirix.Server.UnitTests.Cluster.Replication.LeaderAuthorityDeadlineTests;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A node whose local apply is blocked does not campaign, but still votes and accepts appends, and campaigns again once it recovers.</summary>
[Immutable]
public sealed class ApplyBlockedElectionTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Three = ["n2", "n1", "n3"];

    /// <summary>While the apply is blocked the node sends no pre-vote and no vote, however many election timeouts pass, and keeps its term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedNodeDoesNotCampaign(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        var termBefore = scope.State.Term;
        scope.State.ApplyBlock.Block();

        scope.Time.Advance(Options.ElectionTimeout);
        var first = await election.StepAsync(cancellationToken);
        var delay = election.NextDelay();
        scope.Time.Advance(Options.ElectionTimeout);
        var second = await election.StepAsync(cancellationToken);

        _ = await Assert.That(first).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, termBefore, FailoverDenial.ApplyBlocked));
        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(delay).IsEqualTo(Options.ElectionTimeout);
        _ = await Assert.That(votes.Calls.Length).IsEqualTo(0);
        _ = await Assert.That((scope.State.Role, scope.State.Term)).IsEqualTo((ReplicaGroupRole.Follower, termBefore));
    }

    /// <summary>Once the apply recovers, the node campaigns at its next election timeout and wins.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CampaignsAgainAfterRecovery(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.State.ApplyBlock.Block();
        scope.Time.Advance(Options.ElectionTimeout);
        var blocked = await election.StepAsync(cancellationToken);
        var callsWhileBlocked = votes.Calls.Length;
        scope.State.ApplyBlock.Clear();

        scope.Time.Advance(Options.ElectionTimeout - Tick);
        var waiting = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That((blocked.Denial, callsWhileBlocked, waiting.Event)).IsEqualTo((FailoverDenial.ApplyBlocked, 0, ElectionEvent.None));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Authorized);
        _ = await Assert.That(votes.Calls.Length).IsGreaterThan(0);
    }

    /// <summary>A copy that is not complete is denied for that reason, whether or not its apply is blocked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierDenialsKeepPrecedence(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant), seeded: false);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.State.ApplyBlock.Block();

        scope.Time.Advance(Options.ElectionTimeout);
        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.Denied);
        _ = await Assert.That(outcome.Denial).IsEqualTo(FailoverDenial.Unseeded);
    }

    /// <summary>After it sat out a round, a blocked node still grants the pre-vote and the vote of a candidate, through the follower path of its group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedNodeStillGrantsBallots(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-blocked-ballots");
        await SeededLogs.SeedGroupsAsync(dir, ["n2"], cancellationToken);
        var time = new FakeTimeProvider();
        await using var registry = new ReplicaGroupRegistry(
            dir,
            ["n2"],
            3,
            Fingerprint,
            1UL,
            NullLoggerFactory.Instance,
            new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System })
        {
            Election = Options,
            ElectionClock = time,
        };
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog("n2", out var log);
        var state = registry.StateFor("n2");
        var election = new ReplicaGroupElection(
            state,
            log!,
            new ScriptedVotes(Grant),
            new RecordingLeadership(state),
            Three,
            new ReplicaRpcHeader("n2", Fingerprint, 1UL, 0UL, string.Empty, "n1"));
        _ = await election.StepAsync(cancellationToken);
        state.ApplyBlock.Block();
        time.Advance(Options.ElectionTimeout);
        var denied = await election.StepAsync(cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var preVote = await follower.PreVoteAsync("n2", Fingerprint, 1UL, new ElectionVoteRequest("n3", 8UL, 0UL, 0UL), cancellationToken);
        var vote = await follower.RequestVoteAsync("n2", Fingerprint, 1UL, new ElectionVoteRequest("n3", 8UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That(denied.Denial).IsEqualTo(FailoverDenial.ApplyBlocked);
        _ = await Assert.That(preVote.Granted).IsTrue();
        _ = await Assert.That(vote.Granted).IsTrue();
    }

    /// <summary>An apply that blocks while the pre-vote runs ends the candidacy before the own vote: no term is made durable and no vote is asked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockDuringPreVoteEndsCandidacy(CancellationToken cancellationToken)
    {
        ReplicaApplyBlock? block = null;
        var votes = new ScriptedVotes(call =>
        {
            block?.Block();
            return Grant(call);
        });
        await using var scope = await OpenAsync("n2", 3, votes);
        block = scope.State.ApplyBlock;
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        var before = await scope.Log.GetStatusAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        var after = await scope.Log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, before.CurrentTerm, FailoverDenial.ApplyBlocked));
        _ = await Assert.That(after.CurrentTerm).IsEqualTo(before.CurrentTerm);
        _ = await Assert.That(Array.FindIndex(votes.Calls, static call => !call.PreVote)).IsEqualTo(-1);
        _ = await Assert.That(scope.State.Role).IsEqualTo(ReplicaGroupRole.Follower);
    }

    /// <summary>An apply that blocks while the vote round runs keeps a won term from being promoted; the durable term stays.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockDuringVoteKeepsTheWinFromLeading(CancellationToken cancellationToken)
    {
        ReplicaApplyBlock? block = null;
        var votes = new ScriptedVotes(call =>
        {
            if (!call.PreVote)
                block?.Block();

            return Grant(call);
        });
        await using var scope = await OpenAsync("n2", 3, votes);
        block = scope.State.ApplyBlock;
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        var after = await scope.Log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, after.CurrentTerm, FailoverDenial.ApplyBlocked));
        _ = await Assert.That(after.CurrentTerm).IsGreaterThan(1UL);
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo(string.Empty);
        _ = await Assert.That(scope.State.Role).IsEqualTo(ReplicaGroupRole.Follower);
    }

    /// <summary>Before the apply loop looked at the group the node waits: no pre-vote, no vote, no term; after the first decision it campaigns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnassessedNodeWaitsForTheApplyLoop(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(Grant);
        await using var scope = await OpenAsync("n2", 3, votes, assessed: false);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        var before = await scope.Log.GetStatusAsync(cancellationToken);

        scope.Time.Advance(Options.ElectionTimeout);
        var first = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);
        var second = await election.StepAsync(cancellationToken);
        var callsWhileWaiting = votes.Calls.Length;
        var during = await scope.Log.GetStatusAsync(cancellationToken);
        var delay = election.NextDelay();
        scope.State.ApplyBlock.Clear();
        scope.Time.Advance(Options.HeartbeatInterval - Tick);
        var early = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Tick);
        var campaigned = await election.StepAsync(cancellationToken);

        _ = await Assert.That(delay).IsEqualTo(Options.HeartbeatInterval);
        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That(first).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, before.CurrentTerm, FailoverDenial.ApplyUnassessed));
        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(callsWhileWaiting).IsEqualTo(0);
        _ = await Assert.That((during.CurrentTerm, during.VotedFor)).IsEqualTo((before.CurrentTerm, before.VotedFor));
        _ = await Assert.That(campaigned.Event).IsEqualTo(ElectionEvent.Authorized);
    }

    /// <summary>The owner of a group does not lead it at its provisional term before the apply loop looked at it, nor while the apply is blocked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerWaitsForTheApplyLoopToLead(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n1", 3, new ScriptedVotes(Grant), assessed: false);
        var election = CreateElection(scope, ["n1", "n2", "n3"]);

        var unassessed = await election.StepAsync(cancellationToken);
        scope.State.ApplyBlock.Block();
        var blocked = await election.StepAsync(cancellationToken);
        var callsWhileHeld = scope.Leadership.Calls;
        scope.State.ApplyBlock.Clear();
        var started = await election.StepAsync(cancellationToken);

        _ = await Assert.That(unassessed.Denial).IsEqualTo(FailoverDenial.ApplyUnassessed);
        _ = await Assert.That(blocked.Denial).IsEqualTo(FailoverDenial.ApplyBlocked);
        _ = await Assert.That(callsWhileHeld).IsEqualTo(string.Empty);
        _ = await Assert.That(started.Event).IsEqualTo(ElectionEvent.Authorized);
        _ = await Assert.That(scope.State.Role).IsEqualTo(ReplicaGroupRole.Leader);
    }
}
