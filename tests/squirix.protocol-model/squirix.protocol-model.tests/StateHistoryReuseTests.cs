using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class StateHistoryReuseTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ChangedNodesRecordNewEvidence(bool removeMessage)
    {
        var state = CommittedLeader();
        var leader = new NodeState(0, NodeRole.Leader, 3, 0, [new LogEntry(2, 1), new LogEntry(3, 2)], NodeRuntime.Create(2, 2, 0, 0, 0, false), false);
        NodeState[] nodes = [leader, state.Nodes[1], state.Nodes[2]];
        var next = removeMessage ? state.WithoutMessage(0, nodes) : state.WithNodes(nodes);

        _ = await Assert.That(next.CommittedEntries.Count).IsEqualTo(2);
        _ = await Assert.That(next.CommittedEntries[0]).IsEqualTo(new ModelCommitDecision(1, 2, 2));
        _ = await Assert.That(next.CommittedEntries[1]).IsEqualTo(new ModelCommitDecision(2, 3, 3));
        _ = await Assert.That(next.ElectedLeaders.Count).IsEqualTo(2);
        _ = await Assert.That(next.ElectedLeaders[0]).IsEqualTo(new ModelElectionRecord(2, 0));
        _ = await Assert.That(next.ElectedLeaders[1]).IsEqualTo(new ModelElectionRecord(3, 0));
        _ = await Assert.That(next.Messages.Count).IsEqualTo(removeMessage ? 0 : 1);
        _ = await Assert.That(state.CommittedEntries.Count).IsEqualTo(1);
        _ = await Assert.That(state.ElectedLeaders.Count).IsEqualTo(1);
    }

    [Test]
    public async Task ReplacementHistoryIncludesLiveLeader()
    {
        var state = CommittedLeader();
        var next = state.With(elections: [new ModelElectionRecord(1, 1)]);

        _ = await Assert.That(next.Nodes).IsSameReferenceAs(state.Nodes);
        _ = await Assert.That(next.CommittedEntries).IsSameReferenceAs(state.CommittedEntries);
        _ = await Assert.That(next.ElectedLeaders.Count).IsEqualTo(2);
        _ = await Assert.That(next.ElectedLeaders[0]).IsEqualTo(new ModelElectionRecord(1, 1));
        _ = await Assert.That(next.ElectedLeaders[1]).IsEqualTo(new ModelElectionRecord(2, 0));
        _ = await Assert.That(state.ElectedLeaders.Count).IsEqualTo(1);
        _ = await Assert.That(state.ElectedLeaders[0]).IsEqualTo(new ModelElectionRecord(2, 0));
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    public async Task UnchangedNodesKeepHistory(int change)
    {
        var state = CommittedLeader();
        var before = state.Fingerprint(false);
        var next = change switch
        {
            0 => state.WithMessages([], 11),
            1 => state.WithPartitions([0, 1, 0]),
            2 => state.WithMatchIndexes([1, 1, 0]),
            3 => state.WithoutMessage(0),
            4 => state.WithNodes([state.Nodes[0], state.Nodes[1], state.Nodes[2]]),
            _ => throw new InvalidOperationException("Unsupported state change."),
        };

        _ = await Assert.That(state.CommittedEntries.Count).IsEqualTo(1);
        _ = await Assert.That(state.ElectedLeaders.Count).IsEqualTo(1);
        _ = await Assert.That(next.Nodes).IsSameReferenceAs(state.Nodes);
        _ = await Assert.That(next.CommittedEntries).IsSameReferenceAs(state.CommittedEntries);
        _ = await Assert.That(next.ElectedLeaders).IsSameReferenceAs(state.ElectedLeaders);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    private static ClusterState CommittedLeader()
    {
        var leader = new NodeState(0, NodeRole.Leader, 2, 0, [new LogEntry(2, 1)], NodeRuntime.Create(1, 1, 0, 0, 0, false), false);
        return ClusterState.CreateInitial(3).WithNodes([leader, NodeState.CreateInitial(1), NodeState.CreateInitial(2)])
            .WithMessages([new InFlightMessage(10, MessagePayload.VoteResponse(1, 0, 2, true))], 11);
    }
}
