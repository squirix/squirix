using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds each way a hint can be wrong to a refusal, one test per way.</summary>
/// <remarks>
/// Fixed group and topology: three members that can all reach each other, no reads, no crashes, no partitions. A hint is
/// authenticated by nothing but its sender field, so what these tests can ask is what the receiver checks, not who signed
/// the message; the emission side is the policy checker's question, not this one's.
/// </remarks>
public sealed class ProtocolHintRejectionTests
{
    /// <summary>A hint naming a term its sender has not reached is refused.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintForATermTheSenderNeverReached()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var refused = ExploreRunner.NextStates(TransferFixtures.HintState(TransferFixtures.FirstFollowerId, TransferFixtures.SecondFollowerId, TransferFixtures.SecondTerm), profile, BrokenMode.None);
        var honoured = ExploreRunner.NextStates(TransferFixtures.HintState(TransferFixtures.LeaderId, TransferFixtures.SecondFollowerId, TransferFixtures.SecondTerm), profile, BrokenMode.None);

        _ = await Assert.That(TransferFixtures.CountAtTerm(refused, TransferFixtures.SecondFollowerId, TransferFixtures.ThirdTerm)).IsEqualTo(0)
            .Because("node 1 is at term 1 and cannot hand over term 2");
        _ = await Assert.That(TransferFixtures.CountAtTerm(honoured, TransferFixtures.SecondFollowerId, TransferFixtures.ThirdTerm)).IsGreaterThan(0)
            .Because("the same hint from a leader of that term is honoured, so the refusal above is the sender check");
    }

    /// <summary>A hint that names its own sender is refused.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintNamingItsOwnSender()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var refused = ExploreRunner.NextStates(TransferFixtures.HintState(TransferFixtures.FirstFollowerId, TransferFixtures.FirstFollowerId, TransferFixtures.ElectedTerm), profile, BrokenMode.None);
        var honoured = ExploreRunner.NextStates(TransferFixtures.HintState(TransferFixtures.LeaderId, TransferFixtures.FirstFollowerId, TransferFixtures.ElectedTerm), profile, BrokenMode.None);

        _ = await Assert.That(TransferFixtures.CountAtTerm(refused, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0);
        _ = await Assert.That(TransferFixtures.CountAtTerm(honoured, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsGreaterThan(0);
    }

    /// <summary>A hint to a member that is already campaigning is refused: the campaign it would start has started.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintToACampaigningMember()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var refused = ExploreRunner.NextStates(TransferFixtures.HintToMemberInRole(NodeRole.Candidate), profile, BrokenMode.None);
        var honoured = ExploreRunner.NextStates(TransferFixtures.HintToMemberInRole(NodeRole.Follower), profile, BrokenMode.None);

        _ = await Assert.That(TransferFixtures.CountAtTerm(refused, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsEqualTo(0)
            .Because("a candidate is not named again: it has already started the campaign");
        _ = await Assert.That(TransferFixtures.CountAtTerm(honoured, TransferFixtures.FirstFollowerId, TransferFixtures.SecondTerm)).IsGreaterThan(0)
            .Because("the same hint to a follower starts one, so the refusal above is the role check");
    }

    /// <summary>A hint to a member that is leading is refused.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintToALeadingMember()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var successors = ExploreRunner.NextStates(TransferFixtures.HintToTheLeaderState(), profile, BrokenMode.None);

        // A retirement may demote the leader, which is a legal step of its own; what the named leader must not do is campaign.
        for (var i = 0; i < successors.Count; i++)
        {
            var named = successors[i].Nodes[TransferFixtures.LeaderId];
            _ = await Assert.That(named.Role).IsNotEqualTo(NodeRole.Candidate).Because("the named member leads already, so it does not campaign");
            _ = await Assert.That(named.CurrentTerm).IsEqualTo(TransferFixtures.ElectedTerm);
        }
    }

    /// <summary>A hint to an unseeded member is refused: a copy that lost its disk takes neither the hint nor the campaign.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintToAnUnseededMember()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var successors = ExploreRunner.NextStates(TransferFixtures.HintToAnUnseededState(), profile, BrokenMode.None);

        for (var i = 0; i < successors.Count; i++)
        {
            var receiver = successors[i].Nodes[TransferFixtures.SecondFollowerId];
            _ = await Assert.That(receiver.Unseeded).IsTrue();
            _ = await Assert.That(receiver.CurrentTerm).IsEqualTo(0);
        }
    }
}
