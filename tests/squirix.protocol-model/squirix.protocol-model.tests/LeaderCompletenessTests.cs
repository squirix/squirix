using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Leader completeness is independent of unrelated terms and excludes later-term commits.</summary>
public sealed class LeaderCompletenessTests
{
    /// <summary>A higher-term member cannot hide an incomplete leader; later-term entries do not constrain that leader.</summary>
    /// <param name="entryTerm">The committed entry's term.</param>
    /// <param name="candidate">Whether the higher-term member is campaigning.</param>
    /// <param name="missing">Whether the leader must contain the entry.</param>
    [Test]
    [Arguments(1, true, true)]
    [Arguments(2, true, true)]
    [Arguments(3, true, false)]
    [Arguments(1, false, true)]
    public async Task HigherTermDoesNotHideLeader(int entryTerm, bool candidate, bool missing)
    {
        var committed = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Follower, entryTerm, -1, [new LogEntry(entryTerm, 1)], NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            NodeState.CreateInitial(1),
            NodeState.CreateInitial(2),
        ]);
        var state = committed.WithNodes(
        [
            new NodeState(0, NodeRole.Follower, 3, -1, [new LogEntry(entryTerm, 1)], NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(1, NodeRole.Leader, 2, 1, [], in NodeRuntime.Initial, false),
            new NodeState(2, candidate ? NodeRole.Candidate : NodeRole.Follower, 3, 2, [], in NodeRuntime.Initial, false),
        ]);
        var violation = SafetyChecker.Check(state);
        _ = await Assert.That(violation?.Invariant).IsEqualTo(missing ? "LeaderCompleteness" : null);
        if (missing)
            _ = await Assert.That(violation!.Detail).IsEqualTo($"Leader 1 missing committed entry {entryTerm}@1");
    }

    /// <summary>A complete older leader remains safe while a newer leader commits its new entry.</summary>
    [Test]
    public async Task OlderLeaderIgnoresLaterTermCommit()
    {
        LogEntry[] prefix = [new(1, 1)];
        LogEntry[] extended = [new(1, 1), new(2, 2)];
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Leader, 1, 0, prefix, NodeRuntime.Create(1, 1, 3, 0, 0, false), false),
            new NodeState(1, NodeRole.Leader, 2, 1, extended, NodeRuntime.Create(2, 2, 6, 0, 0, false), false),
            new NodeState(2, NodeRole.Candidate, 3, 2, extended, NodeRuntime.Create(2, 2, 0, 0, 0, false), false),
        ]);
        _ = await Assert.That(SafetyChecker.Check(state)).IsNull();
    }
}
