using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds the hint to the emission policy and to the scope of the campaign it starts.</summary>
public sealed class ProtocolHintPolicyTests
{
    /// <summary>The emission policy names the fault the safety invariants cannot see.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The counterexample is the point: a policy violation carries the state, the event that produced it and the path to
    /// it, so a reader can tell which node sent what, without the report claiming the data was endangered.
    /// </remarks>
    [Test]
    public async Task UnretiredHintBreaksTheEmissionPolicy()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var sound = ExploreRunner.Run(profile, BrokenMode.None);
        var broken = ExploreRunner.Run(profile, BrokenMode.TransferHint);

        _ = await Assert.That(sound.PolicyViolation).IsNull();
        _ = await Assert.That(broken.PolicyViolation).IsNotNull();
        _ = await Assert.That(broken.PolicyViolation!.Policy).IsEqualTo(ProtocolPolicyChecker.HintSourceRetired);
        _ = await Assert.That(broken.PolicyViolation.Detail).Contains("did not retire");
        _ = await Assert.That(broken.PolicyViolation.EventTrace).Contains("senderRole=Leader");
        _ = await Assert.That(broken.PolicyViolatingState).IsNotNull();
        _ = await Assert.That(broken.CounterexamplePaths).IsNotNull();
        _ = await Assert.That(broken.CounterexamplePaths!.Count > 1).IsTrue();

        // The finding is a policy one, and it stays out of the safety field: no invariant is claimed to be broken.
        _ = await Assert.That(broken.Violation).IsNull();
    }

    /// <summary>The exception belongs to the member the hint names: no other member campaigns while a leader is heard.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// This is what "a hint is the only way past the leader-contact rule" means as a test. The leader-contact rule is
    /// modelled on the candidate — a member that hears a live leader does not campaign — and not on the voter, whose
    /// refusal the product also applies; the omission only adds grants, so it cannot hide a safety violation, and it is
    /// why nothing here speaks about disruption. The test's claim is about scope: the exception must not be reachable by
    /// any campaign a hint did not start. The control is the same state with the hint naming the other member, where that
    /// member campaigns and the first one does not.
    /// </remarks>
    [Test]
    public async Task OnlyTheNamedMemberMayCampaign()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var namedSecond = ExploreRunner.NextStates(TransferFixtures.ServingLeaderState(TransferFixtures.SecondFollowerId), profile, BrokenMode.None);
        var namedFirst = ExploreRunner.NextStates(TransferFixtures.ServingLeaderState(TransferFixtures.FirstFollowerId), profile, BrokenMode.None);
        var nobody = ExploreRunner.NextStates(TransferFixtures.ServingLeaderState(TransferFixtures.NoHint), profile, BrokenMode.None);

        _ = await Assert.That(TransferFixtures.CountAtTerm(nobody, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0);
        _ = await Assert.That(TransferFixtures.CountAtTerm(nobody, TransferFixtures.SecondFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0);

        _ = await Assert.That(TransferFixtures.CountAtTerm(namedSecond, TransferFixtures.SecondFollowerId, TransferFixtures.SecondTerm)).IsGreaterThan(0)
            .Because("the named member must campaign");
        _ = await Assert.That(TransferFixtures.CountAtTerm(namedSecond, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0)
            .Because("an unnamed member must not");

        _ = await Assert.That(TransferFixtures.CountAtTerm(namedFirst, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsGreaterThan(0);
        _ = await Assert.That(TransferFixtures.CountAtTerm(namedFirst, TransferFixtures.SecondFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0);
    }

    /// <summary>The campaign a hint starts is an ordinary one: it collects votes and does not become a leader in the same step.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>Quorum, vote persistence and the log comparison are the vote rules; the hint only decides who campaigns.</remarks>
    [Test]
    public async Task HintCampaignCollectsOrdinaryVotes()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var successors = ExploreRunner.NextStates(TransferFixtures.ServingLeaderState(TransferFixtures.FirstFollowerId), profile, BrokenMode.None);

        var campaigning = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            var candidate = successors[i].Nodes[TransferFixtures.FirstFollowerId];
            if (candidate.Role != NodeRole.Candidate)
                continue;

            campaigning++;

            // One vote, its own, and a request to each of the others: nothing about the hint shortens the round.
            _ = await Assert.That(VoteMask.CountGranted(candidate.VotesGranted)).IsEqualTo(1);
            _ = await Assert.That(successors[i].Nodes[TransferFixtures.LeaderId].Role).IsEqualTo(NodeRole.Leader)
                .Because("the serving leader is not deposed by the campaign the hint started");

            var requests = 0;
            for (var m = 0; m < successors[i].Messages.Count; m++)
            {
                if (successors[i].Messages[m].Kind == MsgKind.RequestVote && successors[i].Messages[m].From == TransferFixtures.FirstFollowerId)
                    requests++;
            }

            _ = await Assert.That(requests).IsEqualTo(2);
        }

        _ = await Assert.That(campaigning).IsGreaterThan(0).Because("the hint must start the campaign");
    }
}
