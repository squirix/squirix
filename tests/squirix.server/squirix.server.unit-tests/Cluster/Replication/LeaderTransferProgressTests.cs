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

/// <summary>A state transfer that makes progress holds the authority deadline of its leader without moving the window; one that stops making progress holds nothing.</summary>
[Immutable]
public sealed class LeaderTransferProgressTests : ServerUnitTestBase
{
    /// <summary>A transfer whose replies keep arriving holds the deadline for as long as it runs, far past the window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProgressingTransferHoldsTheDeadline(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);

        var outcome = await ProgressForAsync(scope, election, TimeSpan.FromMinutes(4), cancellationToken);

        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.PromotionPending);
        _ = await Assert.That((scope.State.Role, scope.State.HasAuthority)).IsEqualTo((ReplicaGroupRole.Leader, false));
    }

    /// <summary>A transfer that made progress once and then stuck holds the deadline for one election timeout only: the leader gives up at the window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StuckTransferNoLongerHoldsTheDeadline(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        scope.State.TransferProgress.Record();
        var early = await LeadForAsync(scope, election, TimeSpan.FromSeconds(30) - Tick, cancellationToken);

        var outcome = await LeadForAsync(scope, election, Tick, cancellationToken);

        _ = await Assert.That(early.Event).IsEqualTo(ElectionEvent.PromotionPending);
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.SteppedDown, 2UL, FailoverDenial.None, TimeSpan.FromSeconds(30)));
    }

    /// <summary>The window is not reset by a transfer: once the progress stops, the deadline that has long passed fires within one election timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferDoesNotResetTheWindow(CancellationToken cancellationToken)
    {
        await using var scope = await OpenAsync("n2", 3, new ScriptedVotes(Grant));
        scope.Leadership.PromotionsPending = true;
        var election = await ElectAsync(scope, cancellationToken);
        var progressing = await ProgressForAsync(scope, election, TimeSpan.FromSeconds(45), cancellationToken);

        var outcome = await LeadForAsync(scope, election, Options.ElectionTimeout + (Slice * 2), cancellationToken);

        _ = await Assert.That(progressing.Event).IsEqualTo(ElectionEvent.PromotionPending);
        _ = await Assert.That(outcome.Event).IsEqualTo(ElectionEvent.SteppedDown);
        _ = await Assert.That(outcome.Waited).IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(45));
        _ = await Assert.That(outcome.Waited).IsLessThan(TimeSpan.FromSeconds(45) + Options.ElectionTimeout + (Slice * 3));
    }

    private static async Task<ElectionOutcome> ProgressForAsync(ElectionScope scope, ReplicaGroupElection election, TimeSpan span, CancellationToken cancellationToken)
    {
        var outcome = new ElectionOutcome(ElectionEvent.None, scope.State.Term);
        for (var left = span; left > TimeSpan.Zero && outcome.Event != ElectionEvent.SteppedDown; left -= Slice)
        {
            scope.State.RecordFollowerContact(0, scope.State.Term);
            scope.State.TransferProgress.Record();
            scope.Time.Advance(Slice);
            outcome = await election.StepAsync(cancellationToken);
        }

        return outcome;
    }
}
