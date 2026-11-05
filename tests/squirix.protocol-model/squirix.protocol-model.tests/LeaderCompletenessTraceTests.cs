using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Reachable missing-prefix elections masked by an unrelated higher-term candidate.</summary>
public sealed class LeaderCompletenessTraceTests
{
    /// <summary>A missing committed prefix is detected even when another member has a higher term.</summary>
    /// <param name="higherCandidate">Whether C first advances to term four.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MissingPrefixSurvivesHigherCandidate(bool higherCandidate)
    {
        var profile = ExploreProfile.ForLogRules(4, 2, 2, 300_000);
        var state = ClusterState.CreateInitial(3);
        state = Pick(state, static s => s.Nodes[0].Role == NodeRole.Leader, "elect A in term one");
        state = DropAll(state);
        state = Pick(state, static s => s.Nodes[0].LastLogIndex == 2, "append A's second entry");
        state = DropAll(state);
        state = Pick(state, static s => s.Nodes[1].Role == NodeRole.Leader && s.Nodes[1].CurrentTerm == 2, "B wins term two with C");
        state = Pick(state, static s => s.Messages is [{ To: 2 } _] && s.Nodes[0].LastLogIndex == 2, "drop B's append to A");
        state = Pick(state, static s => s.Nodes[2].LastLogTerm == 2 && s.Messages.Count == 1, "C stores B's entry");
        state = Pick(state, static s => s.Nodes[1].CommitIndex == 1, "B commits term two entry");
        state = DropAll(state);
        state = Pick(state, static s => s.Nodes[0].Role == NodeRole.Follower, "A crashes");
        state = Pick(state, static s => s.Nodes[0].Role == NodeRole.Candidate && s.Nodes[0].CurrentTerm == 2, "A loses term two");
        state = Pick(state, static s => s.Nodes[0].Role == NodeRole.Follower && s.Nodes[0].CurrentTerm == 2, "A crashes again");
        state = Pick(state, static s => s.Nodes[1].Role == NodeRole.Follower && s.Nodes[0].Role == NodeRole.Follower && s.Nodes[2].Role == NodeRole.Follower, "B crashes with committed entry retained");
        if (higherCandidate)
        {
            state = Pick(state, static s => s.Nodes[2].Role == NodeRole.Candidate && s.Nodes[2].CurrentTerm == 3, "C loses term three");
            state = Pick(state, static s => s.Nodes[2].Role == NodeRole.Candidate && s.Nodes[2].CurrentTerm == 4, "C loses term four");
        }

        _ = await Assert.That(SafetyChecker.Check(state)).IsNull();
        state = Pick(state, static s => s.Nodes[0].Role == NodeRole.Leader && s.Nodes[0].CurrentTerm == 3, "A wins term three with B under broken log order");
        _ = await Assert.That(state.Nodes[1].CommitIndex).IsEqualTo(1);
        _ = await Assert.That(state.Nodes[1].LogEntries[0].Term).IsEqualTo(2);
        _ = await Assert.That(state.Nodes[0].LogEntries[0].Term).IsEqualTo(1);
        _ = await Assert.That(SafetyChecker.Check(state)?.Invariant).IsEqualTo("LeaderCompleteness");

        ClusterState Pick(ClusterState before, Func<ClusterState, bool> predicate, string step)
        {
            return Select(before, profile, predicate, step);
        }

        ClusterState DropAll(ClusterState before)
        {
            var current = before;
            while (current.Messages.Count > 0)
                current = SelectDrop(current, profile);

            return current;
        }
    }

    private static bool SameNodes(ClusterState before, ClusterState after)
    {
        for (var i = 0; i < before.Nodes.Count; i++)
        {
            if (!ReferenceEquals(before.Nodes[i], after.Nodes[i]))
                return false;
        }

        return true;
    }

    private static ClusterState Select(ClusterState before, ExploreProfile profile, Func<ClusterState, bool> predicate, string step)
    {
        if (SafetyChecker.Check(before) != null)
            throw new InvalidOperationException("Prefix already violates a checked invariant before " + step);

        var successors = ExploreRunner.NextStates(before, profile, BrokenMode.LogOrder);
        for (var i = 0; i < successors.Count; i++)
        {
            if (predicate(successors[i]))
                return successors[i];
        }

        throw new InvalidOperationException("No actual transition for " + step);
    }

    private static ClusterState SelectDrop(ClusterState before, ExploreProfile profile)
    {
        var successors = ExploreRunner.NextStates(before, profile, BrokenMode.LogOrder);
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Messages.Count == before.Messages.Count - 1 && SameNodes(before, successors[i]))
                return successors[i];
        }

        throw new InvalidOperationException("No actual drop transition");
    }
}
