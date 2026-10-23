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

/// <summary>The election driver holds back on a copy that is not complete: it leads, campaigns, and votes only over a complete one.</summary>
[Immutable]
public sealed class ReplicaGroupElectionSeedingTests : ServerUnitTestBase
{
    private static readonly string[] OwnFirst = ["n1", "n2", "n3"];
    private static readonly string[] Three = ["n2", "n1", "n3"];

    /// <summary>The own group over an unseeded copy is not led at term one, and every step reports the same denial.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededOwnGroupIsNotLed(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n1", 3, new ScriptedVotes(static _ => null), null, false);
        var election = CreateElection(scope, OwnFirst);

        var first = await election.StepAsync(cancellationToken);
        var second = await election.StepAsync(cancellationToken);

        var denied = new ElectionOutcome(ElectionEvent.Denied, 0UL, FailoverDenial.Unseeded);
        _ = await Assert.That((first, second)).IsEqualTo((denied, denied));
        _ = await Assert.That(election.NextDelay()).IsEqualTo(Options.HeartbeatInterval);
        _ = await Assert.That((scope.State.Role, scope.Leadership.Calls)).IsEqualTo((ReplicaGroupRole.Follower, string.Empty));
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(0UL);
    }

    /// <summary>The own group is led at term one as soon as its copy is complete, the way it was before copies could be unseeded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteOwnGroupLeadsAtTermOne(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n1", 3, new ScriptedVotes(static _ => null), null, false);
        var election = CreateElection(scope, OwnFirst);
        var held = await election.StepAsync(cancellationToken);
        await scope.Log.CompleteBootstrapAsync(cancellationToken);

        var led = await election.StepAsync(cancellationToken);

        _ = await Assert.That(held.Event).IsEqualTo(ElectionEvent.Denied);
        _ = await Assert.That(led).IsEqualTo(new ElectionOutcome(ElectionEvent.Authorized, 1UL));
        _ = await Assert.That(scope.Leadership.Calls).IsEqualTo("promote:1");
    }

    /// <summary>A copy of another group that is not complete does not campaign once the election timeout fires, asks nobody for a vote, and reports the denial.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededCopyDoesNotCampaign(CancellationToken cancellationToken)
    {
        var votes = new ScriptedVotes(static _ => null);
        await using var scope = await OpenAsync("n2", 3, votes, null, false);
        var election = CreateElection(scope, Three);
        _ = await election.StepAsync(cancellationToken);
        scope.Time.Advance(Options.ElectionTimeout);

        var outcome = await election.StepAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, 0UL, FailoverDenial.Unseeded));
        _ = await Assert.That(votes.Calls.Length).IsEqualTo(0);
        _ = await Assert.That((await scope.Log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(election.NextDelay()).IsEqualTo(Options.ElectionTimeout);
    }

    /// <summary>The own vote is cast only over a complete copy that has not reached the term.</summary>
    /// <returns>A task that completes when the assertions finished.</returns>
    [Test]
    public async Task OnlyCompleteCopyCastsOwnVote()
    {
        var complete = Status(GroupMemberState.Complete, FollowerLogReadiness.Ready, 3UL);

        _ = await Assert.That(complete.BlocksVote(4UL)).IsFalse();
        _ = await Assert.That(complete.BlocksVote(3UL)).IsTrue();
        _ = await Assert.That((complete with { MemberState = GroupMemberState.Unseeded }).BlocksVote(4UL)).IsTrue();
        _ = await Assert.That((complete with { MemberState = GroupMemberState.Incomplete }).BlocksVote(4UL)).IsTrue();
        _ = await Assert.That((complete with { Readiness = FollowerLogReadiness.Failed }).BlocksVote(4UL)).IsTrue();
    }

    private static FollowerLogStatus Status(GroupMemberState state, FollowerLogReadiness readiness, ulong term) =>
        new("n2", ReadOnlyMemory<byte>.Empty, 1UL, term, string.Empty, 0UL, 0UL, 0UL, 0UL, readiness, state);
}
