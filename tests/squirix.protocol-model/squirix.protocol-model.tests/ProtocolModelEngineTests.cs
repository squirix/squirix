using System;
using Xunit;

namespace Squirix.ProtocolModel.Tests;

public static class ProtocolModelEngineTests
{
    private static readonly LogEntry[] SingleEntryLog = [new(1, 1)];

    [Fact]
    public static void ExplorerRejectsBrokenTermCommitRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.CurrentTermCommit);
        Assert.NotNull(result.Violation);
        Assert.Equal("CurrentTermCommit", result.Violation.Invariant);
    }

    [Fact]
    public static void ExplorerRejectsBrokenReadIndexRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.ReadIndex);
        Assert.NotNull(result.Violation);
        Assert.Equal("ReadIndex", result.Violation.Invariant);
    }

    [Fact]
    public static void ExplorerRejectsBrokenVoteRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.Vote);
        Assert.NotNull(result.Violation);
        Assert.Equal("ElectionSafety", result.Violation.Invariant);
    }

    [Fact]
    public static void ReducedAndUnreducedSearchAgree()
    {
        var reduced = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.None);
        var unreduced = ExploreRunner.Run(ExploreProfile.SmallElection(false), BrokenMode.None);

        Assert.Null(reduced.Violation);
        Assert.Null(unreduced.Violation);
        Assert.True(reduced.FixedPointReached);
        Assert.True(unreduced.FixedPointReached);
        Assert.True(unreduced.StatesVisited >= reduced.StatesVisited);

        var brokenReduced = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.Vote);
        var brokenUnreduced = ExploreRunner.Run(ExploreProfile.SmallElection(false), BrokenMode.Vote);
        Assert.NotNull(brokenReduced.Violation);
        Assert.NotNull(brokenUnreduced.Violation);
        Assert.Equal(brokenReduced.Violation.Invariant, brokenUnreduced.Violation.Invariant);
    }

    [Fact]
    public static void FingerprintIsLabelInvariantForVoteMasks()
    {
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Leader, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0b110, 0, 0, false, false)),
            new NodeState(1, NodeRole.Follower, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false, false)),
            new NodeState(2, NodeRole.Follower, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false, false)),
        ]);

        // Same cluster with replica ids rotated 0->1, 1->2, 2->0.
        var rotated = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Follower, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false, false)),
            new NodeState(1, NodeRole.Leader, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0b101, 0, 0, false, false)),
            new NodeState(2, NodeRole.Follower, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false, false)),
        ]);

        Assert.Equal(state.Fingerprint(true), rotated.Fingerprint(true), StringComparer.Ordinal);
        Assert.NotEqual(state.Fingerprint(false), rotated.Fingerprint(false), StringComparer.Ordinal);
    }
}
