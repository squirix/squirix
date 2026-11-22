using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Election safety is read from the election history, so a retirement cannot hide a second election.</summary>
/// <remarks>
/// The checker used to read the roles the nodes currently hold, which answers "are two nodes leaders at once". Election
/// safety is the wider claim that one term has had one leader, and the two come apart at the retirement: a leader that
/// leaves the role becomes a follower and keeps its term, so it disappears from the set the old check read exactly when
/// the second election happens. Each test below walks the shipped transfer profile through real transitions, and the
/// sound vote rule is the control: if it did not also elect one leader, the witness would be about the schedule rather
/// than about the fault.
/// </remarks>
public sealed class ElectionHistoryTests
{
    /// <summary>The evidence of an election outlives the leader that resigned, which is the fact the old checker lost.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvidenceSurvivesRetirement()
    {
        var states = FollowSchedule(BrokenMode.Vote);
        var retired = states[5];
        _ = await Assert.That(retired.Nodes[0].Role).IsEqualTo(NodeRole.Follower);
        _ = await Assert.That(retired.Nodes[0].CurrentTerm).IsEqualTo(1);

        var kept = 0;
        for (var i = 0; i < retired.ElectedLeaders.Count; i++)
        {
            if (retired.ElectedLeaders[i].Term == 1 && retired.ElectedLeaders[i].NodeId == 0)
                kept++;
        }

        _ = await Assert.That(kept).IsEqualTo(1).Because("node 0 led term 1 before it retired; a follower role does not undo that");
        _ = await Assert.That(SafetyChecker.Check(retired)).IsNull();
    }

    /// <summary>Two states that differ only in their election evidence must not share a key, or the exploration merges them.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// This is the deduplication half of the fix. If the evidence stayed out of the fingerprint, the state in which node 0
    /// led term 1 and retired would be merged with the state in which it never led, and the second election would be
    /// reached from a state that no longer remembers the first.
    /// </remarks>
    [Test]
    public async Task HistorySeparatesStates()
    {
        var withoutEvidence = RetiredLayout().With(elections: []);
        var withEvidence = RetiredLayout().With(elections: [new ModelElectionRecord(1, 0)]);
        _ = await Assert.That(withEvidence.Fingerprint(false)).IsNotEqualTo(withoutEvidence.Fingerprint(false));
        _ = await Assert.That(withEvidence.Fingerprint(true)).IsNotEqualTo(withoutEvidence.Fingerprint(true));
    }

    /// <summary>Relabelling the members relabels the evidence, so the reduced key stays a function of the state.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The control is the unreduced pair: the two states must differ there, otherwise the labels were never carried into
    /// the key and the reduced comparison could not have failed.
    /// </remarks>
    [Test]
    public async Task RenamedHistoryKeepsReducedKey()
    {
        var original = LeaderInTermOne(0).With(elections: [new ModelElectionRecord(1, 0)]);
        var renamed = LeaderInTermOne(2).With(elections: [new ModelElectionRecord(1, 2)]);
        _ = await Assert.That(renamed.Fingerprint(false)).IsNotEqualTo(original.Fingerprint(false));
        _ = await Assert.That(renamed.Fingerprint(true)).IsEqualTo(original.Fingerprint(true));
    }

    /// <summary>Two distinct nodes elected in one term are a violation after the first one has retired.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SequentialElectionAfterRetirement()
    {
        var states = FollowSchedule(BrokenMode.Vote);
        var last = states[^1];
        _ = await Assert.That(last.Nodes[0].Role).IsEqualTo(NodeRole.Follower);
        _ = await Assert.That(last.Nodes[2].Role).IsEqualTo(NodeRole.Leader);

        var violation = SafetyChecker.Check(last);
        _ = await Assert.That(violation?.Invariant).IsEqualTo("ElectionSafety");
        _ = await Assert.That(violation!.Detail).IsEqualTo("Two leaders in term 1: 0 and 2");
    }

    /// <summary>The sound vote rule refuses the second vote, so the same schedule elects one leader and stays safe.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SoundRuleElectsOneLeader()
    {
        var states = FollowSchedule(BrokenMode.None);
        var last = states[^1];
        _ = await Assert.That(last.Nodes[2].Role).IsEqualTo(NodeRole.Candidate);
        _ = await Assert.That(SafetyChecker.Check(last)).IsNull();

        var elected = 0;
        for (var i = 0; i < last.ElectedLeaders.Count; i++)
        {
            if (last.ElectedLeaders[i].Term == 1 && last.ElectedLeaders[i].NodeId == 0)
                elected++;
        }

        _ = await Assert.That(elected).IsEqualTo(1);
        _ = await Assert.That(last.ElectedLeaders.Count).IsEqualTo(1);
    }

    /// <summary>Repeated evidence for the same leader is safe even when records of another term separate it.</summary>
    [Test]
    public async Task UnorderedHistoryAllowsSameLeader()
    {
        ModelElectionRecord[] history = [new(2, 0), new(1, 1), new(2, 0), new(1, 1)];
        var state = ClusterState.CreateInitial(3).With(elections: history);

        _ = await Assert.That(SafetyChecker.Check(state)).IsNull();
        _ = await Assert.That(state.ElectedLeaders[0]).IsEqualTo(new ModelElectionRecord(2, 0));
        _ = await Assert.That(state.ElectedLeaders[1]).IsEqualTo(new ModelElectionRecord(1, 1));
    }

    /// <summary>An unordered history keeps the original first conflicting election, including a conflict before the descent.</summary>
    /// <param name="beforeDescent">Whether the first conflict precedes the first descending term.</param>
    /// <param name="detail">The first conflicting pair in the original history order.</param>
    [Test]
    [Arguments(false, "Two leaders in term 2: 0 and 2")]
    [Arguments(true, "Two leaders in term 2: 1 and 2")]
    public async Task UnorderedHistoryKeepsFirstConflict(bool beforeDescent, string detail)
    {
        ModelElectionRecord[] history = beforeDescent
            ? [new ModelElectionRecord(1, 0), new ModelElectionRecord(2, 1), new ModelElectionRecord(2, 1), new ModelElectionRecord(2, 2), new ModelElectionRecord(1, 2)]
            : [new ModelElectionRecord(2, 0), new ModelElectionRecord(2, 0), new ModelElectionRecord(1, 1), new ModelElectionRecord(2, 2), new ModelElectionRecord(1, 0)];
        var state = ClusterState.CreateInitial(3).With(elections: history);
        var before = state.Fingerprint(false);
        var violation = SafetyChecker.Check(state);

        _ = await Assert.That(violation).IsNotNull();
        _ = await Assert.That(violation!.Invariant).IsEqualTo("ElectionSafety");
        _ = await Assert.That(violation.Detail).IsEqualTo(detail);
        _ = await Assert.That(violation.StateFingerprint).IsEqualTo(before);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    /// <summary>Walks the shipped transfer profile through the schedule that elects a second leader in term one.</summary>
    /// <param name="broken">The injected fault.</param>
    /// <returns>Every state the schedule passes through, in order.</returns>
    /// <exception cref="InvalidOperationException">The profile offers no transition for one of the steps.</exception>
    /// <remarks>
    /// Every step is selected from the transitions the profile itself offers, so the schedule witnesses a reachable
    /// execution rather than a state assembled by hand.
    /// </remarks>
    private static List<ClusterState> FollowSchedule(BrokenMode broken)
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var states = new List<ClusterState> { ClusterState.CreateInitial(3) };
        var doubleVotes = broken == BrokenMode.Vote;

        Pick(states, static s => s.Nodes[0].Role == NodeRole.Candidate && s.Nodes[0].CurrentTerm == 1, "A campaigns");
        Pick(states, static s => HasReply(s, 1, 0, true), "B votes for A");
        Pick(states, static s => s.Nodes[0].Role == NodeRole.Leader, "A wins");
        Pick(states, static s => s.Messages.Count == 0, "drop A's undelivered vote request to C");
        Pick(states, static s => s.Nodes[0].Role == NodeRole.Follower && HasHint(s), "A retires");
        Pick(states, static s => s.Messages.Count == 0, "drop the transfer hint");
        Pick(states, static s => s.Nodes[2].Role == NodeRole.Candidate && s.Nodes[2].CurrentTerm == 1, "C campaigns in term 1");
        Pick(states, s => HasReply(s, 1, 2, doubleVotes), "B answers C");
        Pick(states, s => s.Messages.Count == 1 && !HasReply(s, 1, 2, doubleVotes) && (!doubleVotes || s.Nodes[2].Role == NodeRole.Leader), "deliver B's answer to C");
        return states;

        void Pick(List<ClusterState> path, Func<ClusterState, bool> predicate, string step)
        {
            var successors = ExploreRunner.NextStates(path[^1], profile, broken);
            for (var i = 0; i < successors.Count; i++)
            {
                if (!predicate(successors[i]))
                    continue;

                path.Add(successors[i]);
                return;
            }

            throw new InvalidOperationException("The profile offers no transition for: " + step);
        }
    }

    private static bool HasHint(ClusterState state)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message is { Kind: MsgKind.TransferHint, From: 0 })
                return true;
        }

        return false;
    }

    private static bool HasReply(ClusterState state, int from, int to, bool success)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == MsgKind.VoteResponse && message.From == from && message.To == to && message.Success == success)
                return true;
        }

        return false;
    }

    /// <summary>One member leading term one and the other two following, with the votes on the leading member.</summary>
    /// <param name="leaderId">The member that leads.</param>
    /// <returns>The state.</returns>
    private static ClusterState LeaderInTermOne(int leaderId)
    {
        var nodes = new NodeState[3];
        for (var i = 0; i < nodes.Length; i++)
        {
            nodes[i] = i == leaderId
                ? new NodeState(leaderId, NodeRole.Leader, 1, leaderId, [], NodeRuntime.Create(0, 0, 1 << leaderId, 0, 0, false), false)
                : new NodeState(i, NodeRole.Follower, 1, leaderId, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), false);
        }

        return ClusterState.CreateInitial(3).WithNodes(nodes);
    }

    /// <summary>Three followers in term one, with no leader, so only the evidence can tell two such states apart.</summary>
    /// <returns>The state.</returns>
    private static ClusterState RetiredLayout() => ClusterState.CreateInitial(3).WithNodes(
    [
        new NodeState(0, NodeRole.Follower, 1, 0, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), false),
        new NodeState(1, NodeRole.Follower, 1, -1, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), false),
        new NodeState(2, NodeRole.Follower, 1, -1, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), false),
    ]);
}
