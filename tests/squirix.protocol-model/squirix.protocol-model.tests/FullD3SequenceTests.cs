using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Drives the whole D3 sequence through <see cref="ExploreRunner.NextStates" />, one delivery at a time.</summary>
/// <remarks>
/// <para>
/// The other probes in this suite build a receiver that already knows its leader, so they hold the source check without
/// ever exercising the path that produces the knowledge. Measured on this head, disabling
/// <c lang="csharp">RecordKnownLeader</c> left every one of the 103 tests green, which is the gap this class closes: here
/// the knowledge is made by delivering an append-entries, and nothing about it is written into the fixture.
/// </para>
/// <para>
/// The sequence is four deliveries and no shortcuts. A leader appends to one follower; that follower takes the append and
/// learns who leads the term; the leader takes the acknowledgement, which is what leaves the profile room for another
/// message; the leader retires and names that follower; and the follower campaigns on the hint it was named in. Each step
/// is a call to the explorer, so every field the assertions read was written by a transition rather than by a constructor.
/// </para>
/// </remarks>
public sealed class FullD3SequenceTests
{
    private const int AppendMessageId = 1;
    private const int AppendedEntryIndex = 2;
    private const int ElectedTerm = 1;
    private const int FirstEntryIndex = 1;
    private const int LeaderId = 0;
    private const int LeaderVotes = 0b111;
    private const int NextTerm = 2;
    private const int NoLeaderId = -1;
    private const int NoLeaderTerm = 0;
    private const int NoVote = -1;
    private const int NoVotes = 0;
    private const int OtherId = 2;
    private const int ReceiverId = 1;

    private static readonly LogEntry[] SingleEntryLog = [new(ElectedTerm, FirstEntryIndex)];

    /// <summary>A follower learns its leader by taking an append-entries, then campaigns on the hint that leader leaves.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The assertions are ordered as the deliveries happen, so a failure names the step that broke rather than the last
    /// number in the sequence.
    /// </remarks>
    [Test]
    public async Task FullD3SequenceThroughNextStates()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();

        // Step 1: the leader leads the current term and an append-entries to B is in flight.
        var beforeDelivery = LeaderAppendingToReceiver();
        _ = await Assert.That(beforeDelivery.Nodes[ReceiverId].KnownLeaderTerm).IsEqualTo(NoLeaderTerm)
            .Because("the fixture must not pre-populate the knowledge, or the delivery below proves nothing");
        _ = await Assert.That(beforeDelivery.Nodes[ReceiverId].KnownLeaderId).IsEqualTo(NoLeaderId)
            .Because("the fixture must not pre-populate the knowledge, or the delivery below proves nothing");

        // Step 1, delivered: taking the append is what teaches the receiver who leads the term.
        var learned = DeliveredAppend(beforeDelivery, profile);
        _ = await Assert.That(learned.Nodes[ReceiverId].KnownLeaderTerm).IsEqualTo(ElectedTerm)
            .Because("the append came from the term the receiver is in, so delivering it records that term");
        _ = await Assert.That(learned.Nodes[ReceiverId].KnownLeaderId).IsEqualTo(LeaderId)
            .Because("the receiver learned the sender from the message, not from the fixture");
        _ = await Assert.That(AppendResponsesTo(learned, LeaderId)).IsEqualTo(1)
            .Because("taking the append enqueues exactly one acknowledgement to its sender, so this successor is that delivery");

        // Step 2: the leader takes the acknowledgement, which leaves the profile no message in flight.
        var acknowledged = DeliveredAppendResponse(learned, profile);
        _ = await Assert.That(acknowledged.Messages.Count).IsEqualTo(0)
            .Because("the acknowledgement is what frees the in-flight budget the retirement and the campaign both need");
        _ = await Assert.That(acknowledged.Nodes[ReceiverId].KnownLeaderId).IsEqualTo(LeaderId)
            .Because("taking the acknowledgement changes the leader's bookkeeping, and the receiver's knowledge must survive it");

        // Step 3: the leader retires and names the follower it replicated furthest, which is B.
        var retiring = RetiredNamingReceiver(acknowledged, profile);
        _ = await Assert.That(retiring.Nodes[LeaderId].Role).IsEqualTo(NodeRole.Follower)
            .Because("a retirement is a step down, and the hint is sent after it");
        _ = await Assert.That(retiring.Nodes[ReceiverId].KnownLeaderTerm).IsEqualTo(ElectedTerm)
            .Because("the receiver's knowledge survives the retirement; that is the field D3 reads when the hint arrives");
        _ = await Assert.That(retiring.Nodes[ReceiverId].KnownLeaderId).IsEqualTo(LeaderId)
            .Because("the hint is attributable only while the receiver still names this sender as the term's leader");

        var hint = HintTo(retiring, ReceiverId);
        _ = await Assert.That(hint.From).IsEqualTo(LeaderId).Because("the leaving leader is the one that names a successor");
        _ = await Assert.That(hint.Term).IsEqualTo(ElectedTerm).Because("a hint carries the term its sender is leaving");

        // Step 4: delivering that exact hint starts the campaign, so the successor sits in the next term.
        var campaigned = CampaignAfterHint(retiring, profile);
        _ = await Assert.That(campaigned.Nodes[ReceiverId].Role).IsEqualTo(NodeRole.Candidate)
            .Because("the hint names this member and it took the leadership the sender left");
        _ = await Assert.That(campaigned.Nodes[ReceiverId].CurrentTerm).IsEqualTo(NextTerm)
            .Because("a campaign is fought in the term after the one it leaves");
        _ = await Assert.That(campaigned.Nodes[ReceiverId].VotedFor).IsEqualTo(ReceiverId)
            .Because("a candidate votes for itself in the term it campaigns in");
        _ = await Assert.That(CountHints(campaigned)).IsEqualTo(0)
            .Because("the campaign state consumed the hint, so it is the delivery and not a timeout that ran alongside it");

        // The negative control varies attribution and nothing else: the hint stays in flight, so the profile's in-flight
        // bound refuses a timeout campaign in both states and the only difference left is whether the hint can be
        // attributed. Taking the hint away instead would empty the flight, legalise a timeout, and measure the budget.
        var unattributable = CountCampaigns(ExploreRunner.NextStates(WithoutKnownLeader(retiring), profile, BrokenMode.None));
        _ = await Assert.That(unattributable).IsEqualTo(0)
            .Because($"the same state with the knowledge cleared produced {unattributable} campaigns, so the campaign above needs the learning");
    }

    /// <summary>Counts the acknowledgements in flight to one member.</summary>
    /// <param name="state">The state.</param>
    /// <param name="sender">The member they are addressed to.</param>
    /// <returns>The count.</returns>
    private static int AppendResponsesTo(ClusterState state, int sender)
    {
        var count = 0;
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.AppendResponse && state.Messages[i].To == sender)
                count++;
        }

        return count;
    }

    /// <summary>Delivers the hint and returns the successor in which the receiver campaigns.</summary>
    /// <param name="state">The state holding the hint.</param>
    /// <param name="profile">The profile.</param>
    /// <returns>The successor.</returns>
    /// <exception cref="AssertionException">No successor campaigns the receiver, so the hint was refused.</exception>
    private static ClusterState CampaignAfterHint(ClusterState state, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[ReceiverId].CurrentTerm == NextTerm)
                return successors[i];
        }

        throw new AssertionException($"no successor moved the receiver to the next term; {successors.Count} successors");
    }

    /// <summary>Counts the successors in which the receiver has moved to the next term.</summary>
    /// <param name="successors">The successors.</param>
    /// <returns>The count.</returns>
    private static int CountCampaigns(List<ClusterState> successors)
    {
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[ReceiverId].CurrentTerm == NextTerm)
                count++;
        }

        return count;
    }

    /// <summary>Counts the hints left in flight.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The count.</returns>
    private static int CountHints(ClusterState state)
    {
        var count = 0;
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.TransferHint)
                count++;
        }

        return count;
    }

    /// <summary>Delivers the append-entries and returns the successor in which the receiver has learned its leader.</summary>
    /// <param name="state">The state holding the append.</param>
    /// <param name="profile">The profile.</param>
    /// <returns>The successor.</returns>
    /// <exception cref="AssertionException">No successor records the sender, so the learning path is not running.</exception>
    private static ClusterState DeliveredAppend(ClusterState state, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        for (var i = 0; i < successors.Count; i++)
        {
            var receiver = successors[i].Nodes[ReceiverId];
            if (receiver is { KnownLeaderTerm: ElectedTerm, KnownLeaderId: LeaderId })
                return successors[i];
        }

        throw new AssertionException($"no successor recorded the sender as the term's leader; {successors.Count} successors, and delivering the append is what should record it");
    }

    /// <summary>Delivers the acknowledgement and returns the successor with no message left in flight.</summary>
    /// <param name="state">The state holding the acknowledgement.</param>
    /// <param name="profile">The profile.</param>
    /// <returns>The successor.</returns>
    /// <exception cref="AssertionException">No successor empties the flight, so the sequence cannot go on.</exception>
    private static ClusterState DeliveredAppendResponse(ClusterState state, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Messages.Count == 0)
                return successors[i];
        }

        throw new AssertionException($"no successor both emptied the flight and recorded the match index; {successors.Count} successors");
    }

    /// <summary>Tells whether a state holds a hint addressed to a member.</summary>
    /// <param name="state">The state.</param>
    /// <param name="receiver">The member.</param>
    /// <returns>True when it does.</returns>
    private static bool HasHintTo(ClusterState state, int receiver)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.TransferHint && state.Messages[i].To == receiver)
                return true;
        }

        return false;
    }

    /// <summary>Returns the hint addressed to a member.</summary>
    /// <param name="state">The state holding the hint.</param>
    /// <param name="receiver">The member it names.</param>
    /// <returns>The hint.</returns>
    /// <exception cref="AssertionException">No hint is addressed to that member.</exception>
    private static InFlightMessage HintTo(ClusterState state, int receiver)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.TransferHint && state.Messages[i].To == receiver)
                return state.Messages[i];
        }

        throw new AssertionException($"no transfer hint is addressed to node {receiver}");
    }

    /// <summary>A leader at the current term with an append-entries to one follower in flight, and nothing else learned.</summary>
    /// <returns>The state.</returns>
    /// <remarks>
    /// The follower's runtime is the profile's ordinary one, so both knowledge fields are at their initial values. The
    /// leader's own replication record is deliberately equal for both followers here: it is the acknowledgement in the next
    /// step that makes this follower the freshest one, and the retirement then has only it to name.
    /// </remarks>
    private static ClusterState LeaderAppendingToReceiver()
    {
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(FirstEntryIndex, FirstEntryIndex, LeaderVotes, 0, 0, false), false),
            new NodeState(ReceiverId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(FirstEntryIndex, FirstEntryIndex, NoVotes, 0, 0, false), false),
            new NodeState(OtherId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(FirstEntryIndex, FirstEntryIndex, NoVotes, 0, 0, false), false),
        ])
        .WithMatchIndexes([FirstEntryIndex, FirstEntryIndex, FirstEntryIndex]);

        return state.WithMessages(
            [new InFlightMessage(AppendMessageId, MessagePayload.Append((LeaderId, ReceiverId, ElectedTerm), new LogEntry(ElectedTerm, AppendedEntryIndex), FirstEntryIndex))],
            AppendMessageId + 1);
    }

    /// <summary>Retires the leader and returns the successor whose hint names the receiver.</summary>
    /// <param name="state">The state whose leader is about to leave.</param>
    /// <param name="profile">The profile.</param>
    /// <returns>The successor.</returns>
    /// <exception cref="AssertionException">No successor sends a hint to the receiver.</exception>
    private static ClusterState RetiredNamingReceiver(ClusterState state, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        for (var i = 0; i < successors.Count; i++)
        {
            var receiver = successors[i].Nodes[ReceiverId];
            if (receiver.KnownLeaderTerm != ElectedTerm || receiver.KnownLeaderId != LeaderId)
                continue;

            if (HasHintTo(successors[i], ReceiverId))
                return successors[i];
        }

        throw new AssertionException($"no successor retired the leader with a hint naming the receiver; {successors.Count} successors");
    }

    /// <summary>Returns the same state with the receiver's leader knowledge cleared, which is the control's only change.</summary>
    /// <param name="state">The state holding the hint.</param>
    /// <returns>The state with the knowledge removed.</returns>
    private static ClusterState WithoutKnownLeader(ClusterState state)
    {
        var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
        nodes[ReceiverId] = ModelTransitionUtil.Patch(
            nodes[ReceiverId],
            new NodePatch { KnownLeaderTerm = NoLeaderTerm, KnownLeaderId = NoLeaderId });
        return state.WithNodes(nodes);
    }
}
