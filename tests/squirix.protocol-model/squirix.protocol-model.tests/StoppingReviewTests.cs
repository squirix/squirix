using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Independent review probes for shutdown admission and its process-lifecycle boundary.</summary>
/// <remarks>
/// <para>
/// These three probes come from the review of this branch and check the fix from angles the schedule-based tests in
/// <see cref="TransferAdrReviewTests" /> do not: every reachable state rather than one walk, the hint path rather than the
/// timeout path, and the restart that is supposed to reopen admission rather than only the retirement that closes it.
/// </para>
/// <para>
/// The restart probe also pins what a restart is in this model. It is not only the loss of a disk and the repair that
/// follows: the crash transition rebuilds the runtime value from the durable state, and the rebuilt value carries no
/// stop, so a crash with an intact disk already reopens admission.
/// </para>
/// </remarks>
public sealed class StoppingReviewTests
{
    /// <summary>A delivered hint cannot bypass stopped-process admission; a running receiver campaigns.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The two states differ in one flag only, so the pair says the hint path reads the stop rather than that the two
    /// states are unrelated. The running state is the positive control: if it stopped campaigning too, the zero below
    /// would be a property of the fixture instead of the guard.
    /// </remarks>
    [Test]
    public async Task HintCannotReopenStoppingReceiver()
    {
        var running = TransferFixtures.ServingLeaderState(1);
        var nodes = ModelTransitionUtil.CloneNodes(running.Nodes);
        nodes[1] = ModelTransitionUtil.Patch(nodes[1], new NodePatch { Stopping = true });
        var stopped = running.WithNodes(nodes);
        _ = await Assert.That(Campaigns(running, 1)).IsGreaterThan(0)
            .Because("a hint naming a running follower has to make it campaign, or the fixture proves nothing");
        _ = await Assert.That(Campaigns(stopped, 1)).IsEqualTo(0)
            .Because("the same hint must not reopen a receiver whose process has stopped");
    }

    /// <summary>No reachable stopped process campaigns or leads in the shipped sound transfer profile.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReachableStoppingNodesStayFollowers()
    {
        var states = ExploreRunner.ReachableStates(ExploreProfile.SmallTransferOneMoreTerm(), BrokenMode.None);
        var stopping = 0;
        var reopened = 0;
        for (var i = 0; i < states.Count; i++)
            for (var node = 0; node < states[i].Nodes.Count; node++)
            {
                if (!states[i].Nodes[node].Stopping)
                    continue;

                stopping++;
                if (states[i].Nodes[node].Role != NodeRole.Follower)
                    reopened++;
            }

        _ = await Assert.That(stopping).IsGreaterThan(0)
            .Because("the profile has to reach a stopped process, or the zero below describes states nobody visited");
        _ = await Assert.That(reopened).IsEqualTo(0)
            .Because("a stopped process stays a follower in every reachable state, not only in the scheduled walk");
    }

    /// <summary>A crash/restart with intact disk clears the volatile stop and permits another campaign.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The schedule walks to a retired source, shows the stop closes its admission, then takes a fault transition that
    /// keeps the disk. The recovered process has to keep its term and its last log index, so the reopen is attributable
    /// to the cleared flag rather than to a reset of the state around it. This is not a full recovered-leadership or
    /// commit test; it checks that another campaign is possible at all.
    /// </remarks>
    /// <exception cref="InvalidOperationException">A required sound transition is unavailable.</exception>
    [Test]
    public async Task RestartReopensRetiredSource()
    {
        var retired = RetiredSource();
        _ = await Assert.That(retired.Nodes[0].Stopping).IsTrue()
            .Because("the walk has to have retired the source before a restart can reopen it");
        _ = await Assert.That(Campaigns(retired, 0)).IsEqualTo(0)
            .Because("a retired source must not campaign while its process is still the stopped one");

        var crashes = ExploreProfile.ForReplicaCount(3, 3, 2, 4, 0, true, false);
        var successors = ExploreRunner.NextStates(retired, crashes, BrokenMode.None);
        ClusterState? restarted = null;
        for (var i = 0; i < successors.Count; i++)
        {
            if (!successors[i].Nodes[0].Stopping && !successors[i].Nodes[0].Unseeded)
                restarted = successors[i];
        }

        _ = await Assert.That(restarted).IsNotNull()
            .Because("a crash on an intact disk has to produce a running process again");
        var recovered = restarted!;
        _ = await Assert.That(recovered.Nodes[0].CurrentTerm).IsEqualTo(retired.Nodes[0].CurrentTerm)
            .Because("the restart keeps the term, so the reopen is the flag clearing and not a state reset");
        _ = await Assert.That(recovered.Nodes[0].LastLogIndex).IsEqualTo(retired.Nodes[0].LastLogIndex)
            .Because("the restart keeps the log, so the reopen is the flag clearing and not a state reset");
        _ = await Assert.That(Campaigns(recovered, 0)).IsGreaterThan(0)
            .Because("a fresh process may acquire authority again, which is the boundary the stop is scoped to");
    }

    private static int Campaigns(ClusterState state, int node)
    {
        var successors = ExploreRunner.NextStates(state, ExploreProfile.SmallTransferOneMoreTerm(), BrokenMode.None);
        var campaigns = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[node].Role == NodeRole.Candidate
                && successors[i].Nodes[node].CurrentTerm > state.Nodes[node].CurrentTerm)
                campaigns++;
        }

        return campaigns;
    }

    private static bool HasReply(ClusterState state)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message is { Kind: MsgKind.VoteResponse, From: 1, To: 0, Success: true })
                return true;
        }

        return false;
    }

    private static ClusterState RetiredSource()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var state = ClusterState.CreateInitial(3);
        Pick(static s => s.Nodes[0].Role == NodeRole.Candidate && s.Nodes[0].CurrentTerm == 1);
        Pick(HasReply);
        Pick(static s => s.Nodes[0].Role == NodeRole.Leader);
        Pick(static s => s.Messages.Count == 0);
        Pick(static s => s.Nodes[0].Stopping && s.Nodes[0].Role == NodeRole.Follower);
        Pick(static s => s.Messages.Count == 0);
        return state;

        void Pick(Func<ClusterState, bool> predicate)
        {
            var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
            for (var i = 0; i < successors.Count; i++)
            {
                if (!predicate(successors[i]))
                    continue;

                state = successors[i];
                return;
            }

            throw new InvalidOperationException("The sound profile offers no required transition.");
        }
    }
}
