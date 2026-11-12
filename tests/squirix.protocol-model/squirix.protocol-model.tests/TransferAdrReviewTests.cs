using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds the transfer model to the two cases the review named against the graceful-transfer ADR.</summary>
/// <remarks>
/// <para>
/// The first case is D1: a member that retired on a planned stop has closed its admission for the rest of that process
/// lifecycle, so dropping the hint it sent must not hand it back the leadership of a later term. The model demoted it to
/// an ordinary follower, which is a role it can leave again, and nothing recorded that it had stopped.
/// </para>
/// <para>
/// The schedule campaigns, wins term 1, retires and drops the hint. The retired source must then have no
/// transition that campaigns again in term 2.
/// </para>
/// </remarks>
public sealed class TransferAdrReviewTests
{
    private const int FirstTerm = 1;
    private const int SecondTerm = 2;

    /// <summary>A member that retired cannot campaign again before a restart closes and reopens its process.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">A required sound transition is unavailable.</exception>
    [Test]
    public async Task RetiredSourceDoesNotCampaignAgain()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var state = ClusterState.CreateInitial(3);
        state = Pick(state, profile, static s => s.Nodes[0].Role == NodeRole.Candidate && s.Nodes[0].CurrentTerm == FirstTerm);
        state = Pick(state, profile, static s => HasReply(s, 1, 0));
        state = Pick(state, profile, static s => s.Nodes[0].Role == NodeRole.Leader);
        state = Pick(state, profile, static s => s.Messages.Count == 0);
        state = Pick(state, profile, static s => s.Nodes[0].Role == NodeRole.Follower && HasHint(s));
        state = Pick(state, profile, static s => s.Messages.Count == 0);

        _ = await Assert.That(state.Nodes[0].Role).IsEqualTo(NodeRole.Follower)
            .Because("the schedule has to have retired the leader before the campaign it forbids can be attempted");

        _ = await Assert.That(state.Nodes[0].CurrentTerm).IsEqualTo(FirstTerm)
            .Because("the source is still in the term it retired in");

        var campaigns = 0;
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[0].Role == NodeRole.Candidate && successors[i].Nodes[0].CurrentTerm == SecondTerm)
                campaigns++;
        }

        _ = await Assert.That(campaigns).IsEqualTo(0)
            .Because("the source already retired and no restart occurred, so its admission is closed");
    }

    /// <summary>Walks one step of the sound profile to the first successor matching the predicate.</summary>
    /// <param name="before">The state to step from.</param>
    /// <param name="profile">The profile.</param>
    /// <param name="predicate">The property the next state must have.</param>
    /// <returns>The successor that matched.</returns>
    /// <exception cref="InvalidOperationException">The sound profile does not offer the required transition.</exception>
    private static ClusterState Pick(ClusterState before, ExploreProfile profile, Func<ClusterState, bool> predicate)
    {
        var next = ExploreRunner.NextStates(before, profile, BrokenMode.None);
        for (var i = 0; i < next.Count; i++)
        {
            if (predicate(next[i]))
                return next[i];
        }

        throw new InvalidOperationException("The sound transfer profile does not offer the required transition from " + before.Fingerprint(false));
    }

    private static bool HasReply(ClusterState state, int from, int to)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == MsgKind.VoteResponse && message.From == from && message.To == to && message.Success)
                return true;
        }

        return false;
    }

    private static bool HasHint(ClusterState state)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.TransferHint)
                return true;
        }

        return false;
    }
}
