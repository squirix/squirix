using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Commit decision terms survive model transitions and distinguish otherwise identical search states.</summary>
public sealed class ModelCommitHistoryTests
{
    /// <summary>History with a different decision term must not merge, including during symmetry reduction.</summary>
    /// <param name="reduced">Whether node labels are reduced.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CommitHistoryChangesStateIdentity(bool reduced)
    {
        var early = CommittedAt(1);
        var late = CommittedAt(3);
        NodeState[] current =
        [
            Node(0, NodeRole.Follower, 3, 1),
            new(1, NodeRole.Leader, 2, 1, [], in NodeRuntime.Initial, false),
            NodeState.CreateInitial(2),
        ];
        early = early.WithNodes(current);
        late = late.WithNodes(current);
        _ = await Assert.That(early.CommittedEntries[0].CommitTerm).IsEqualTo(1);
        _ = await Assert.That(late.CommittedEntries[0].CommitTerm).IsEqualTo(3);
        _ = await Assert.That(early.Fingerprint(reduced)).IsNotEqualTo(late.Fingerprint(reduced));
        _ = await Assert.That(SafetyChecker.Check(early)?.Invariant).IsEqualTo("LeaderCompleteness");
        _ = await Assert.That(SafetyChecker.Check(late)).IsNull();
    }

    /// <summary>Disk loss cannot erase a fact the checker already observed, even if no surviving node remembers its commit index.</summary>
    [Test]
    public async Task DiskLossKeepsCommitDecision()
    {
        var before = CommittedAt(1);
        var successors = ExploreRunner.NextStates(before, ExploreProfile.SmallDiskLoss(), BrokenMode.None);
        ClusterState? wiped = null;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[0].Unseeded)
                wiped = successors[i];
        }

        _ = await Assert.That(wiped).IsNotNull();
        _ = await Assert.That(wiped!.CommittedEntries.Count).IsEqualTo(1);
        _ = await Assert.That(wiped.CommittedEntries[0]).IsEqualTo(before.CommittedEntries[0]);
        foreach (var node in wiped.Nodes)
            _ = await Assert.That(node.CommitIndex).IsEqualTo(0);
    }

    /// <summary>An earlier-term entry committed only by a later leader does not constrain an already-elected older leader.</summary>
    [Test]
    public async Task EarlierEntryCommittedLaterIsSafe()
    {
        LogEntry[] later = [new(1, 1), new(3, 2)];
        var before = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Leader, 3, 0, later, in NodeRuntime.Initial, false),
            new NodeState(1, NodeRole.Leader, 2, 1, [new LogEntry(2, 1)], in NodeRuntime.Initial, false),
            new NodeState(2, NodeRole.Follower, 3, 0, later, in NodeRuntime.Initial, false),
        ]);
        var after = before.WithNodes(
        [
            new NodeState(0, NodeRole.Leader, 3, 0, later, NodeRuntime.Create(2, 0, 5, 0, 0, false), false),
            before.Nodes[1],
            before.Nodes[2],
        ]);
        _ = await Assert.That(after.CommittedEntries.Count).IsEqualTo(2);
        _ = await Assert.That(after.CommittedEntries[0].EntryTerm).IsEqualTo(1);
        _ = await Assert.That(after.CommittedEntries[0].CommitTerm).IsEqualTo(3);
        _ = await Assert.That(SafetyChecker.Check(after)).IsNull();
    }

    private static ClusterState CommittedAt(int term) => ClusterState.CreateInitial(3).WithNodes(
    [
        Node(0, NodeRole.Follower, term, 1),
        Node(1, NodeRole.Follower, term, 0),
        NodeState.CreateInitial(2),
    ]);

    private static NodeState Node(int id, NodeRole role, int term, int committed) =>
        new(id, role, term, -1, [new LogEntry(1, 1)], NodeRuntime.Create(committed, committed, 0, 0, 0, false), false);
}
