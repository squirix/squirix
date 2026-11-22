using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Checks the volatile authorization for an accelerated election.</summary>
public sealed class LeadershipTransferTests : ServerUnitTestBase
{
    /// <summary>Only the known source can trigger one campaign per term.</summary>
    [Test]
    public async Task KnownLeaderWakesOnlyOneCampaign()
    {
        var state = Following();
        _ = await Assert.That(state.Transfers.AcceptTransfer("other", 7)).IsFalse();
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 6)).IsFalse();
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 7)).IsTrue();
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 7)).IsFalse();
        _ = await Assert.That(state.Transfers.TakeTransfer(7)).IsEqualTo("leader");
        _ = await Assert.That(state.Transfers.TakeTransfer(7)).IsNull();
        state.BecomeFollower(8, true);
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 7)).IsFalse();
    }

    /// <summary>Local eligibility and a higher observed term fence the pending attempt.</summary>
    [Test]
    public async Task HigherTermAndBlockedApplyPreventCampaign()
    {
        var state = Following();
        state.ApplyBlock.Block();
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 7)).IsFalse();
        state.ApplyBlock.Clear();
        _ = await Assert.That(state.Transfers.AcceptTransfer("leader", 7)).IsTrue();
        state.ObserveHigherTerm(8);
        _ = await Assert.That(state.Transfers.TakeTransfer(7)).IsNull();
    }

    /// <summary>Recent contact still protects ordinary and mismatched pre-votes.</summary>
    [Test]
    public async Task PreVoteRequiresKnownSourceAndNextTerm()
    {
        var state = Following();
        _ = await Assert.That(state.Transfers.RefusesPreVote(null, 7, 8)).IsTrue();
        _ = await Assert.That(state.Transfers.RefusesPreVote("other", 7, 8)).IsTrue();
        _ = await Assert.That(state.Transfers.RefusesPreVote("leader", 7, 9)).IsTrue();
        _ = await Assert.That(state.Transfers.RefusesPreVote("leader", 7, 8)).IsFalse();
        state.ObserveHigherTerm(8);
        _ = await Assert.That(state.Transfers.RefusesPreVote("leader", 7, 8)).IsTrue();
    }

    /// <summary>A delayed retirement prevents emission until admission is closed and the leader has drained.</summary>
    [Test]
    public async Task RetirementFencesSingleFreshestHint()
    {
        var votes = new ElectionTestDoubles.ScriptedVotes(static _ => null);
        await using var scope = await ElectionTestDoubles.OpenAsync("n1", 3, votes);
        var election = ElectionTestDoubles.CreateElection(scope, ["n1", "n2", "n3"]);
        _ = await election.StepAsync(CancellationToken.None);
        _ = await Assert.That(scope.State.HasAuthority).IsTrue();
        scope.Leadership.MatchIndexes[2] = 3;
        scope.State.ElectableContacts.Record(1, true);
        scope.State.ElectableContacts.Record(2, true);
        var drained = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        scope.Leadership.RetireHold = drained;
        var stopping = ReplicaGroupElection.GracefulStop.StopAndTransferAsync(election);
        _ = await Assert.That(scope.State.HasAuthority).IsFalse();
        _ = await Assert.That(scope.State.GrantAuthority(scope.State.Term)).IsFalse();
        _ = await Assert.That(votes.TransferTargets.Count).IsEqualTo(0);
        await ReplicaGroupElection.GracefulStop.StopAndTransferAsync(election);
        drained.SetResult(true);
        await stopping;
        await ReplicaGroupElection.GracefulStop.StopAndTransferAsync(election);
        _ = await Assert.That(votes.TransferTargets.Count).IsEqualTo(1);
        _ = await Assert.That(votes.TransferTargets[0]).IsEqualTo("n3");
    }

    /// <summary>Recent equally matched candidates yield one target, while expired candidates receive no hint.</summary>
    /// <param name="expired">Whether to expire both candidates before stopping the leader.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TiedAndExpiredCandidates(bool expired)
    {
        var votes = new ElectionTestDoubles.ScriptedVotes(static _ => null);
        await using var scope = await ElectionTestDoubles.OpenAsync("n1", 3, votes);
        var election = ElectionTestDoubles.CreateElection(scope, ["n1", "n2", "n3"]);
        _ = await election.StepAsync(CancellationToken.None);
        scope.State.ElectableContacts.Record(1, true);
        scope.State.ElectableContacts.Record(2, true);
        if (expired)
            scope.Time.Advance(ElectionTestDoubles.Options.ElectionTimeout);

        await ReplicaGroupElection.GracefulStop.StopAndTransferAsync(election);
        _ = await Assert.That(votes.TransferTargets.Count).IsEqualTo(expired ? 0 : 1);
        if (!expired)
            _ = await Assert.That(votes.TransferTargets[0]).IsEqualTo("n2");
    }

    /// <summary>A busy commit gate uses up the transfer budget without restoring source authority.</summary>
    [Test]
    public async Task GateTimeoutSkipsTransfer()
    {
        var votes = new ElectionTestDoubles.ScriptedVotes(static _ => null);
        await using var scope = await ElectionTestDoubles.OpenAsync("n1", 3, votes);
        var election = ElectionTestDoubles.CreateElection(scope, ["n1", "n2", "n3"]);
        _ = await election.StepAsync(CancellationToken.None);
        scope.State.ElectableContacts.Record(2, true);
        scope.Leadership.RetireHold = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopping = ReplicaGroupElection.GracefulStop.StopAndTransferAsync(election);
        scope.Time.Advance(ElectionTestDoubles.Options.VoteRpcTimeout);
        await stopping;
        _ = await Assert.That(votes.TransferTargets.Count).IsEqualTo(0);
        _ = await Assert.That(scope.State.HasAuthority).IsFalse();
    }

    private static ReplicaGroupState Following()
    {
        var state = new ReplicaGroupState(3, new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMinutes(1) }, TimeProvider.System);
        state.SetElectionDriven(true);
        state.BecomeFollower(7, false);
        state.ObserveLeaderContact("leader", 7);
        state.ApplyBlock.Assess();
        return state;
    }
}
