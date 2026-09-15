using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolModelEngineTests
{
    private static readonly LogEntry[] SingleEntryLog = [new(1, 1)];

    [Test]
    public async Task ExplorerRejectsBrokenReadIndexRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.ReadIndex);
        _ = await Assert.That(result.Violation).IsNotNull();
        _ = await Assert.That(result.Violation.Invariant).IsEqualTo("ReadIndex");
    }

    [Test]
    public async Task ExplorerRejectsBrokenTermCommitRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.CurrentTermCommit);
        _ = await Assert.That(result.Violation).IsNotNull();
        _ = await Assert.That(result.Violation.Invariant).IsEqualTo("CurrentTermCommit");
    }

    [Test]
    public async Task ExplorerRejectsBrokenVoteRule()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.Vote);
        _ = await Assert.That(result.Violation).IsNotNull();
        _ = await Assert.That(result.Violation.Invariant).IsEqualTo("ElectionSafety");
    }

    [Test]
    public async Task FingerprintIsLabelInvariantForVoteMasks()
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

        _ = await Assert.That(rotated.Fingerprint(true)).IsEqualTo(state.Fingerprint(true), StringComparer.Ordinal);
        _ = await Assert.That(rotated.Fingerprint(false)).IsNotEqualTo(state.Fingerprint(false), StringComparer.Ordinal);
    }

    [Test]
    public async Task ReducedAndUnreducedSearchAgree()
    {
        var reduced = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.None);
        var unreduced = ExploreRunner.Run(ExploreProfile.SmallElection(false), BrokenMode.None);

        _ = await Assert.That(reduced.Violation).IsNull();
        _ = await Assert.That(unreduced.Violation).IsNull();
        _ = await Assert.That(reduced.FixedPointReached).IsTrue();
        _ = await Assert.That(unreduced.FixedPointReached).IsTrue();
        _ = await Assert.That(unreduced.StatesVisited >= reduced.StatesVisited).IsTrue();

        var brokenReduced = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.Vote);
        var brokenUnreduced = ExploreRunner.Run(ExploreProfile.SmallElection(false), BrokenMode.Vote);
        _ = await Assert.That(brokenReduced.Violation).IsNotNull();
        _ = await Assert.That(brokenUnreduced.Violation).IsNotNull();
        _ = await Assert.That(brokenUnreduced.Violation.Invariant).IsEqualTo(brokenReduced.Violation.Invariant);
    }
}
