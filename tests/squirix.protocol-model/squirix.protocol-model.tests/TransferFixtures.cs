using System.Collections.Generic;

namespace Squirix.ProtocolModel.Tests;

/// <summary>The states the transfer-hint tests start from, and the readings they take of a successor.</summary>
internal static class TransferFixtures
{
    internal const int ElectedTerm = 1;
    internal const int FirstFollowerId = 1;
    internal const int HintMessageId = 1;
    internal const int LeaderId = 0;
    internal const int NoHint = -1;
    internal const int NoVote = -1;
    internal const int SecondFollowerId = 2;
    internal const int SecondTerm = 2;
    internal const int ThirdTerm = 3;

    private static readonly LogEntry[] SingleEntryLog = [new(ElectedTerm, 1)];

    /// <summary>Counts the successors in which a member sits at the given term.</summary>
    /// <param name="successors">The successors.</param>
    /// <param name="nodeId">The member.</param>
    /// <param name="term">The term.</param>
    /// <returns>The count.</returns>
    internal static int CountAtTerm(List<ClusterState> successors, int nodeId, int term)
    {
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[nodeId].CurrentTerm == term)
                count++;
        }

        return count;
    }

    /// <summary>A state whose leader is a term ahead, with a hint in flight from the named sender to the named receiver.</summary>
    /// <param name="from">The sender of the hint.</param>
    /// <param name="to">The receiver.</param>
    /// <param name="hintTerm">The term the hint claims.</param>
    /// <returns>The state.</returns>
    internal static ClusterState HintState(int from, int to, int hintTerm) => ClusterState.CreateInitial(3).WithNodes(
    [
        new NodeState(LeaderId, NodeRole.Leader, SecondTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
        new NodeState(FirstFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, LedBy(1, 1, 0, hintTerm, from), false),
        new NodeState(SecondFollowerId, NodeRole.Follower, SecondTerm, NoVote, SingleEntryLog, LedBy(1, 1, 0, hintTerm, from), false),
    ]).WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(from, to, hintTerm))], HintMessageId + 1);

    /// <summary>A state in which the hint names a copy that has lost its disk.</summary>
    /// <returns>The state.</returns>
    internal static ClusterState HintToAnUnseededState() => ClusterState.CreateInitial(3).WithNodes(
    [
        new NodeState(LeaderId, NodeRole.Follower, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(0, 0, 0, 0, 0, false), false),
        new NodeState(FirstFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(0, 0, 0, 0, 0, false), false),
        new NodeState(SecondFollowerId, NodeRole.Follower, 0, NoVote, [], LedBy(0, 0, 0, ElectedTerm, LeaderId), true),
    ]).WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(LeaderId, SecondFollowerId, ElectedTerm))], HintMessageId + 1);

    /// <summary>A state in which one member is already campaigning and a hint names it.</summary>
    /// <param name="namedRole">The role of the member the hint names.</param>
    /// <returns>The state.</returns>
    internal static ClusterState HintToMemberInRole(NodeRole namedRole)
    {
        var votedFor = namedRole == NodeRole.Candidate ? FirstFollowerId : NoVote;
        var votes = namedRole == NodeRole.Candidate ? 0b010 : 0;
        return ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
            new NodeState(FirstFollowerId, namedRole, ElectedTerm, votedFor, SingleEntryLog, LedBy(1, 1, votes, ElectedTerm, SecondFollowerId), false),
            new NodeState(SecondFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ]).WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(SecondFollowerId, FirstFollowerId, ElectedTerm))], HintMessageId + 1);
    }

    /// <summary>A state in which the hint names the leader itself.</summary>
    /// <returns>The state.</returns>
    internal static ClusterState HintToTheLeaderState() => ClusterState.CreateInitial(3).WithNodes(
    [
        new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, LedBy(1, 1, 0b111, ElectedTerm, FirstFollowerId), false),
        new NodeState(FirstFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        new NodeState(SecondFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
    ]).WithMessages([new InFlightMessage(HintMessageId, MessagePayload.TransferHint(FirstFollowerId, LeaderId, ElectedTerm))], HintMessageId + 1);

    /// <summary>A state in which a serving leader hears its members, with a hint addressed to one of them or none.</summary>
    /// <param name="named">The member the hint names, or <see cref="NoHint" /> for a state with no hint in flight.</param>
    /// <returns>The state.</returns>
    internal static ClusterState ServingLeaderState(int named) =>
        SingleState(named == NoHint ? null : new InFlightMessage(HintMessageId, MessagePayload.TransferHint(LeaderId, named, ElectedTerm)));

    /// <summary>Builds a runtime value for a member that has heard the given leader in the given term.</summary>
    /// <param name="commitIndex">The commit index.</param>
    /// <param name="appliedIndex">The applied index.</param>
    /// <param name="votes">The votes granted.</param>
    /// <param name="leaderTerm">The term the member heard a leader in.</param>
    /// <param name="leaderId">The member it heard.</param>
    /// <returns>The runtime value carrying that knowledge.</returns>
    private static NodeRuntime LedBy(int commitIndex, int appliedIndex, int votes, int leaderTerm, int leaderId) =>
        NodeRuntime.Create(commitIndex, appliedIndex, votes, 0, 0, false) with
        {
            KnownLeaderTerm = leaderTerm,
            KnownLeaderId = leaderId,
        };

    /// <summary>A cluster of three members whose leader holds one entry and voted for itself.</summary>
    /// <param name="hint">The hint in flight, or <see langword="null" />.</param>
    /// <returns>The state.</returns>
    private static ClusterState SingleState(InFlightMessage? hint)
    {
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(LeaderId, NodeRole.Leader, ElectedTerm, LeaderId, SingleEntryLog, NodeRuntime.Create(1, 1, 0b111, 0, 0, false), false),
            new NodeState(FirstFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, LedBy(1, 1, 0, ElectedTerm, LeaderId), false),
            new NodeState(SecondFollowerId, NodeRole.Follower, ElectedTerm, NoVote, SingleEntryLog, LedBy(1, 1, 0, ElectedTerm, LeaderId), false),
        ]);

        return hint == null ? state : state.WithMessages([hint.Value], HintMessageId + 1);
    }
}
