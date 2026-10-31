using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds the leadership-transfer hint to a leaving leader, and checks what the explorer makes of the hint.</summary>
public sealed class ProtocolTransferHintTests
{
    private const int LeaderId = 0;
    private const int FirstFollowerId = 1;
    private const int SecondFollowerId = 2;
    private const int ElectedTerm = 1;
    private const int NoVote = -1;
    private const int SecondTerm = 2;
    private const int ThirdTerm = 3;
    private const int NoHint = 0;
    private const int HintMessageId = 1;

    private static readonly LogEntry[] SingleEntryLog = [new(ElectedTerm, 1)];

    /// <summary>A retirement names exactly one successor, and the leader that named it has already stepped down.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetiringLeaderNamesOneSuccessor()
    {
        var successors = ExploreRunner.NextStates(LeaderState(), ExploreProfile.SmallTransfer(), BrokenMode.None);

        var named = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            var hints = Hints(successors[i]);
            if (hints.Count == 0)
                continue;

            named++;
            _ = await Assert.That(hints.Count).IsEqualTo(1);
            _ = await Assert.That(hints[0].To == hints[0].From).IsFalse();
            _ = await Assert.That(successors[i].Nodes[hints[0].From].Role).IsEqualTo(NodeRole.Follower);
        }

        _ = await Assert.That(named > 0).IsTrue();
    }

    /// <summary>The sound rule produces no hint from a leader that is still serving; the injected fault produces them.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintComesOnlyFromALeavingLeader()
    {
        var profile = ExploreProfile.SmallTransfer();
        var sound = ExploreRunner.NextStates(LeaderState(), profile, BrokenMode.None);
        var broken = ExploreRunner.NextStates(LeaderState(), profile, BrokenMode.TransferHint);

        _ = await Assert.That(CountHintsFromServingLeader(sound)).IsEqualTo(0);
        _ = await Assert.That(CountHintsFromServingLeader(broken) > 0).IsTrue();
    }

    /// <summary>A retiring leader keeps the vote it cast in that term, so a later request in the same term is refused.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The leader voted for itself before it retired. Retiring changes its role, not the term, and a member votes at most
    /// once per term, so the slot must survive the step down; clearing it would let the retired leader grant a second vote
    /// in the term it had already voted in, which is the counterexample this test is named for.
    /// </remarks>
    [Test]
    public async Task RetirementKeepsTheVoteItCastInThatTerm()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var retiring = RetiringLeaderState();
        var retired = NextStatesWhere(retiring, profile, BrokenMode.None, LeaderId, NodeRole.Follower);

        _ = await Assert.That(retired).IsNotNull().Because("a retirement transition must exist");
        _ = await Assert.That(retired.Nodes[LeaderId].CurrentTerm).IsEqualTo(ElectedTerm).Because("retiring does not change the term");
        _ = await Assert.That(retired.Nodes[LeaderId].VotedFor).IsEqualTo(LeaderId).Because("the vote it cast in this term must survive the step down");

        // Behaviour, not just the field: another candidate asking for a vote in the same term must be refused.
        var asking = retired.WithMessages(
            [new InFlightMessage(HintMessageId + 1, MessagePayload.VoteRequest(FirstFollowerId, LeaderId, ElectedTerm, 1, ElectedTerm))],
            HintMessageId + 2);
        var grants = VoteResponsesTo(asking, FirstFollowerId);

        // A refusal must be answered, not merely absent: silence would satisfy "no grant" without the rule working.
        _ = await Assert.That(grants.Count > 0).IsTrue().Because($"the retired leader answered {grants.Count} of the requests");
        _ = await Assert.That(grants.Contains(true)).IsFalse().Because("the retired leader already voted in this term and cannot vote twice");
    }

    /// <summary>Naming a successor must not depend on node ids, because the transfer profile is explored with symmetry reduction.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The two states differ only by swapping the ids of two equally matched followers whose terms differ, so they are one
    /// state under renaming and share a reduced fingerprint. A tie-break on the lower id picks the follower at the leader's
    /// term in one state and the one a term ahead in the other, so the reduced state would stand for two different sets of
    /// transitions and one of them would be dropped. The assertion is that the successors' reduced fingerprints agree.
    /// </remarks>
    [Test]
    public async Task SuccessorIsChosenIndependentlyOfIds()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var first = RetiringLeaderState(SecondTerm, ElectedTerm);
        var renamed = RetiringLeaderState(ElectedTerm, SecondTerm);

        _ = await Assert.That(first.Fingerprint(true)).IsEqualTo(renamed.Fingerprint(true)).Because("the two states are one state under renaming");

        var firstSet = SuccessorFingerprints(first, profile);
        var renamedSet = SuccessorFingerprints(renamed, profile);

        _ = await Assert.That(firstSet.Count > 0).IsTrue().Because("retirement transitions must exist");
        _ = await Assert.That(string.Join('\n', firstSet)).IsEqualTo(string.Join('\n', renamedSet))
            .Because($"renaming must not change the successor set; first={string.Join(',', firstSet)} renamed={string.Join(',', renamedSet)}");
    }

    /// <summary>A hint that arrives in its own term is delivered and starts the campaign, so the delivery path is exercised rather than assumed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintIsDeliveredAndStartsTheCampaign()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var withHint = SuccessorState(true, NodeRole.Follower, ElectedTerm, ElectedTerm);
        var withoutHint = SuccessorState(true, NodeRole.Follower, ElectedTerm, NoHint);

        var delivered = CountAtTerm(ExploreRunner.NextStates(withHint, profile, BrokenMode.None), FirstFollowerId, SecondTerm);
        var baseline = CountAtTerm(ExploreRunner.NextStates(withoutHint, profile, BrokenMode.None), FirstFollowerId, SecondTerm);

        // While the leader still serves, the named member cannot campaign on its own, so the campaign can only come from the hint.
        _ = await Assert.That(baseline).IsEqualTo(0).Because($"baseline without a hint was {baseline}");
        _ = await Assert.That(delivered).IsGreaterThan(0).Because($"delivered campaign count was {delivered}");
    }

    /// <summary>A hint replayed into a member that already campaigned in a later term starts no second term, and the check is what stops it.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The ceiling is three terms on purpose. Under the two-term profile this test cannot fail: the successor sits at the second
    /// term, and honouring a stale hint would need a third one, which the profile refuses to build. The three assertions are a
    /// delivery witness, the replay rule, and the negative control showing that removing the check makes the rule visible.
    /// </remarks>
    [Test]
    public async Task ReplayedHintStartsNoSecondTerm()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var stale = SuccessorState(true, NodeRole.Follower, SecondTerm, ElectedTerm);
        var withoutHint = SuccessorState(true, NodeRole.Follower, SecondTerm, NoHint);

        var baseline = CountAtTerm(ExploreRunner.NextStates(withoutHint, profile, BrokenMode.None), FirstFollowerId, ThirdTerm);
        var sound = CountAtTerm(ExploreRunner.NextStates(stale, profile, BrokenMode.None), FirstFollowerId, ThirdTerm);
        var removed = CountAtTerm(ExploreRunner.NextStates(stale, profile, BrokenMode.TransferHintStaleAccept), FirstFollowerId, ThirdTerm);

        // The same state with the hint in its own term does start the campaign, so silence here is the stale check and not a lost message.
        var inTerm = SuccessorState(true, NodeRole.Follower, SecondTerm, SecondTerm);
        var inTermCount = CountAtTerm(ExploreRunner.NextStates(inTerm, profile, BrokenMode.None), FirstFollowerId, ThirdTerm);
        _ = await Assert.That(inTermCount).IsGreaterThan(baseline).Because($"in-term hint {inTermCount} vs no-hint baseline {baseline}");

        // A duplicate that arrives after the successor campaigned changes nothing.
        _ = await Assert.That(sound).IsEqualTo(baseline).Because($"stale hint {sound} vs no-hint baseline {baseline}");

        // And the check is load-bearing: with it removed the same duplicate starts a second term.
        _ = await Assert.That(removed).IsGreaterThan(baseline).Because($"stale-accepting fault {removed} vs no-hint baseline {baseline}");
    }

    /// <summary>The hint confers nothing: the profile that carries it reaches its fixed point without a safety violation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintBreaksNoSafetyInvariant()
    {
        var sound = ExploreRunner.Run(ExploreProfile.SmallTransfer(), BrokenMode.None);

        _ = await Assert.That(sound.Violation).IsNull();
        _ = await Assert.That(sound.FixedPointReached).IsTrue();
    }

    /// <summary>A hint sent outside a retire reorders the same terms and breaks no invariant either: what it costs is availability.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnretiredHintBreaksNoInvariant()
    {
        var sound = ExploreRunner.Run(ExploreProfile.SmallTransfer(), BrokenMode.None);
        var broken = ExploreRunner.Run(ExploreProfile.SmallTransfer(), BrokenMode.TransferHint);

        _ = await Assert.That(broken.Violation).IsNull();
        _ = await Assert.That(broken.FixedPointReached).IsTrue();

        // The fault is not inert: it reaches the states the sound rule keeps out, and every one of them is safe.
        _ = await Assert.That(broken.StatesVisited > sound.StatesVisited).IsTrue();
    }

    /// <summary>A leader that has replicated one entry to both members, so it has a freshest successor to name.</summary>
    /// <returns>The state.</returns>
    private static ClusterState LeaderState() => ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
            new NodeState(FirstFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(SecondFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ]);

    /// <summary>A leader that has voted for itself in the elected term and replicated to both members equally.</summary>
    /// <param name="firstTerm">The term of the first follower.</param>
    /// <param name="secondTerm">The term of the second follower.</param>
    /// <returns>The state.</returns>
    private static ClusterState RetiringLeaderState(int firstTerm = ElectedTerm, int secondTerm = ElectedTerm) => ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
            new NodeState(FirstFollowerId, NodeRole.Follower, firstTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(SecondFollowerId, NodeRole.Follower, secondTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ])
        .WithMatchIndexes([2, 2, 2]);

    /// <summary>Returns the first successor in which a member holds the given role, or a failure when none does.</summary>
    /// <param name="state">The state.</param>
    /// <param name="profile">The profile.</param>
    /// <param name="broken">The injected fault.</param>
    /// <param name="nodeId">The member.</param>
    /// <param name="role">The role it must hold.</param>
    /// <returns>The successor.</returns>
    /// <exception cref="AssertionException">No successor holds the role; the caller's fixture is wrong.</exception>
    private static ClusterState NextStatesWhere(ClusterState state, ExploreProfile profile, BrokenMode broken, int nodeId, NodeRole role)
    {
        var successors = ExploreRunner.NextStates(state, profile, broken);
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[nodeId].Role == role)
                return successors[i];
        }

        throw new AssertionException($"no successor put node {nodeId} in role {role}; {successors.Count} successors");
    }

    /// <summary>Returns the grant flags of the vote responses the explorer produced for the named sender.</summary>
    /// <param name="state">The state holding exactly one vote request.</param>
    /// <param name="sender">The candidate that asked.</param>
    /// <returns>One entry per response, so an absent response and a refusal are not the same reading.</returns>
    private static List<bool> VoteResponsesTo(ClusterState state, int sender)
    {
        var successors = ExploreRunner.NextStates(state, ExploreProfile.SmallTransferOneMoreTerm(), BrokenMode.None);
        var grants = new List<bool>(successors.Count);
        for (var i = 0; i < successors.Count; i++)
        {
            var messages = successors[i].Messages;
            for (var m = 0; m < messages.Count; m++)
            {
                if (messages[m].Kind == MsgKind.VoteResponse && messages[m].To == sender)
                    grants.Add(messages[m].Success);
            }
        }

        return grants;
    }

    /// <summary>Collects the reduced fingerprints of the retirement successors, which is what a renaming must not change.</summary>
    /// <param name="state">The state.</param>
    /// <param name="profile">The profile.</param>
    /// <returns>The fingerprints, sorted so two runs compare as sets.</returns>
    private static List<string> SuccessorFingerprints(ClusterState state, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        var output = new List<string>(successors.Count);
        for (var i = 0; i < successors.Count; i++)
            output.Add(successors[i].Fingerprint(true));

        output.Sort(StringComparer.Ordinal);
        return output;
    }

    /// <summary>Builds a state in which the first follower has already campaigned once and a hint is in flight, or none when no hint term is given.</summary>
    /// <param name="leaderServing">Whether the first member still leads, which keeps the hint's target from campaigning on its own.</param>
    /// <param name="role">The role of the member the hint names.</param>
    /// <param name="term">The term that member is in.</param>
    /// <param name="hintTerm">The term the leader sent the hint from, or <see cref="NoHint" /> for a state with no hint in flight.</param>
    /// <returns>The state.</returns>
    private static ClusterState SuccessorState(bool leaderServing, NodeRole role, int term, int hintTerm)
    {
        var votedFor = role == NodeRole.Candidate ? FirstFollowerId : NoVote;
        var votes = role == NodeRole.Candidate ? 0b010 : 0;
        var firstRole = leaderServing ? NodeRole.Leader : NodeRole.Follower;
        var firstVote = leaderServing ? LeaderId : NoVote;

        // A leader is only heard by a member in the same term, so a serving leader is placed at the term it has to be heard in.
        var firstTerm = leaderServing ? term : ElectedTerm;
        var state = ClusterState.CreateInitial(3).WithNodes(
            [
                new NodeState(LeaderId, firstRole, firstTerm, firstVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
                new NodeState(FirstFollowerId, role, term, votedFor, SingleEntryLog, NodeRuntime.Create(1, 1, votes, 0, 0, false), false),
                new NodeState(SecondFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            ]);

        return hintTerm == NoHint
            ? state
            : state.WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(LeaderId, FirstFollowerId, hintTerm))], HintMessageId + 1);
    }

    /// <summary>Counts the successors in which a member sits at the given term.</summary>
    /// <param name="successors">The successors.</param>
    /// <param name="nodeId">The member.</param>
    /// <param name="term">The term.</param>
    /// <returns>The count.</returns>
    private static int CountAtTerm(List<ClusterState> successors, int nodeId, int term)
    {
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[nodeId].CurrentTerm == term)
                count++;
        }

        return count;
    }

    /// <summary>Returns the hints in flight, so a test can ask who sent them.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The hints.</returns>
    private static List<InFlightMessage> Hints(ClusterState state)
    {
        var hints = new List<InFlightMessage>(state.Messages.Count);
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Kind == MsgKind.TransferHint)
                hints.Add(state.Messages[i]);
        }

        return hints;
    }

    /// <summary>Counts the successors whose hint comes from a node that is still a leader at the hint's term.</summary>
    /// <param name="successors">The successors.</param>
    /// <returns>The count.</returns>
    private static int CountHintsFromServingLeader(List<ClusterState> successors)
    {
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            var hints = Hints(successors[i]);
            for (var h = 0; h < hints.Count; h++)
            {
                var sender = successors[i].Nodes[hints[h].From];
                if (sender.Role == NodeRole.Leader && sender.CurrentTerm == hints[h].Term)
                    count++;
            }
        }

        return count;
    }
}
