using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds a transfer hint to a sender the receiver can actually attribute, which is the D3 case the review named.</summary>
/// <remarks>
/// <para>
/// The sender's own term says only that it has reached the term, never that it led it. Without a receiver-side check, a
/// member that has reached term t as an ordinary follower can name term t in a hint and start a campaign,
/// which is the wrong-source finding this class is named for.
/// </para>
/// <para>
/// The two states below differ in one thing only: which member the receiver has heard from. Both send a hint from a
/// member that is at the hinted term, so a refusal in the first case cannot be the sender-term rule; and the second case
/// shows the same hint honoured once the receiver knows its sender, so the zero in the first is the knowledge check.
/// </para>
/// </remarks>
public sealed class TransferHintSourceKnowledgeTests
{
    private const int LeaderId = 0;
    private const int NeverLeaderId = 1;
    private const int ReceiverId = 2;
    private const int ElectedTerm = 1;
    private const int SecondTerm = 2;
    private const int NoVote = -1;
    private const int HintMessageId = 1;

    private static readonly LogEntry[] SingleEntryLog = [new(ElectedTerm, 1)];

    /// <summary>A member the receiver never heard lead cannot authorize a hint, though it is at the hinted term.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NeverLeaderCannotAuthorizeHint()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();

        // The sender is the member that never led, and the receiver knows the real leader, so only the knowledge check can refuse it.
        var baseline = CountAtSecondTerm(ExploreRunner.NextStates(KnowledgeState(NeverLeaderId, LeaderId, true), profile, BrokenMode.None));
        var successors = ExploreRunner.NextStates(KnowledgeState(NeverLeaderId, LeaderId, false), profile, BrokenMode.None);
        var refused = CountAtSecondTerm(successors);

        _ = await Assert.That(baseline).IsEqualTo(0)
            .Because($"while the leader still serves the receiver cannot campaign on its own, so any campaign is the hint's; baseline was {baseline}");
        _ = await Assert.That(successors.Count).IsGreaterThan(0)
            .Because("the state has to have successors, or the zero below is a state nobody stepped from");
        _ = await Assert.That(refused).IsEqualTo(0)
            .Because("the sender is at the hinted term but the receiver knows another member as its leader, so the hint is unattributable");
    }

    /// <summary>The same hint is honoured once the receiver knows its sender as that term's leader.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>This is the control for the test above: one field of one member differs, and the campaign appears.</remarks>
    [Test]
    public async Task KnownLeaderCanAuthorizeHint()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var honoured = CountAtSecondTerm(ExploreRunner.NextStates(KnowledgeState(NeverLeaderId, NeverLeaderId, false), profile, BrokenMode.None));

        _ = await Assert.That(honoured).IsGreaterThan(0)
            .Because("the receiver knows this sender as the leader of the hinted term, so the hint is attributable and starts the campaign");
    }

    /// <summary>Builds a state in which the receiver is in the elected term and a hint is in flight, or none.</summary>
    /// <param name="hintFrom">The member that sends the hint.</param>
    /// <param name="knownLeader">The member the receiver knows as the leader of the elected term.</param>
    /// <param name="noHint">Whether the state carries no hint at all.</param>
    /// <returns>The state.</returns>
    /// <remarks>
    /// The sender is placed at the hinted term on purpose: a sender below it would be refused by the sender-term rule, and
    /// the test would then say nothing about the knowledge check it exists to hold.
    /// </remarks>
    private static ClusterState KnowledgeState(int hintFrom, int knownLeader, bool noHint)
    {
        var receiverRuntime = NodeRuntime.Create(1, 1, 0, 0, 0, false) with
        {
            KnownLeaderTerm = ElectedTerm,
            KnownLeaderId = knownLeader,
        };

        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
            new NodeState(NeverLeaderId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(ReceiverId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, in receiverRuntime, false),
        ]);

        return noHint
            ? state
            : state.WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(hintFrom, ReceiverId, ElectedTerm))], HintMessageId + 1);
    }

    private static int CountAtSecondTerm(System.Collections.Generic.List<ClusterState> successors)
    {
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[ReceiverId].CurrentTerm == SecondTerm)
                count++;
        }

        return count;
    }
}
