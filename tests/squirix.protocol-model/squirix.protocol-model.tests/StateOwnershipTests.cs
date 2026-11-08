using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Immutable model states share their own collections and copy mutable input collections.</summary>
public sealed class StateOwnershipTests
{
    /// <summary>External arrays cannot modify either a state or a derived state after construction.</summary>
    [Test]
    public async Task SuppliedCollectionsAreCopied()
    {
        NodeState[] nodes = [NodeState.CreateInitial(0), NodeState.CreateInitial(1), NodeState.CreateInitial(2)];
        InFlightMessage[] messages = [new(1, MessagePayload.VoteRequest(0, 1, 1, 0, 0))];
        int[] partitions = [0, 0, 0];
        int[] match = [0, 0, 0];
        var state = ClusterState.CreateInitial(3).With(nodes, messages, partitions, 2, match);
        var derived = state.WithNodes([nodes[0], nodes[1], nodes[2]]);
        var before = state.Fingerprint(false);
        var derivedBefore = derived.Fingerprint(false);

        nodes[0] = nodes[0].Wiped();
        messages[0] = new InFlightMessage(2, MessagePayload.VoteRequest(2, 1, 3, 0, 0));
        partitions[0] = 1;
        match[0] = 9;

        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
        _ = await Assert.That(derived.Fingerprint(false)).IsEqualTo(derivedBefore);
        _ = await Assert.That(state.Nodes).IsNotSameReferenceAs(nodes);
        _ = await Assert.That(state.Messages).IsNotSameReferenceAs(messages);
        _ = await Assert.That(state.Partitions).IsNotSameReferenceAs(partitions);
        _ = await Assert.That(state.MatchIndexes).IsNotSameReferenceAs(match);
    }

    /// <summary>Unchanged collections stay shared, while replacement collections remain isolated from callers.</summary>
    [Test]
    public async Task DerivedStatesShareOnlyOwnedArrays()
    {
        var state = ClusterState.CreateInitial(3).WithMessages([new InFlightMessage(1, MessagePayload.VoteRequest(0, 1, 1, 0, 0))]);
        var unchanged = state.With();
        _ = await Assert.That(unchanged.Nodes).IsSameReferenceAs(state.Nodes);
        _ = await Assert.That(unchanged.Messages).IsSameReferenceAs(state.Messages);
        _ = await Assert.That(unchanged.Partitions).IsSameReferenceAs(state.Partitions);
        _ = await Assert.That(unchanged.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);

        int[] replacement = [0, 0, 0];
        var changed = state.WithPartitions(replacement);
        replacement[0] = 9;
        _ = await Assert.That(changed.Partitions[0]).IsEqualTo(0);
        _ = await Assert.That(state.Partitions[0]).IsEqualTo(0);
        _ = await Assert.That(changed.Messages).IsSameReferenceAs(state.Messages);

        NodeState[] wiped = [state.Nodes[0].Wiped(), state.Nodes[1], state.Nodes[2]];
        int[] match = [0, 0, 0];
        var lost = state.WithDiskLoss(wiped, match);
        wiped[0] = NodeState.CreateInitial(0);
        match[0] = 9;
        _ = await Assert.That(lost.Nodes[0].Unseeded).IsTrue();
        _ = await Assert.That(lost.MatchIndexes[0]).IsEqualTo(0);
        _ = await Assert.That(lost.DiskLosses).IsEqualTo(state.DiskLosses + 1);
        _ = await Assert.That(lost.Messages).IsSameReferenceAs(state.Messages);
        _ = await Assert.That(lost.Partitions).IsSameReferenceAs(state.Partitions);
    }

    /// <summary>Equal replacements reuse owned arrays without retaining mutable caller arrays.</summary>
    [Test]
    public async Task EqualReplacementsReuseOwnedArrays()
    {
        var state = ClusterState.CreateInitial(3);
        NodeState[] nodes = [state.Nodes[0], state.Nodes[1], state.Nodes[2]];
        int[] partitions = [0, 0, 0];
        int[] match = [0, 0, 0];
        var derived = state.With(nodes, partitions: partitions, matchIndexes: match);
        var before = state.Fingerprint(false);

        _ = await Assert.That(derived.Nodes).IsSameReferenceAs(state.Nodes);
        _ = await Assert.That(derived.Partitions).IsSameReferenceAs(state.Partitions);
        _ = await Assert.That(derived.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);

        nodes[0] = nodes[0].Wiped();
        partitions[0] = 9;
        match[2] = 9;

        _ = await Assert.That(derived.Fingerprint(false)).IsEqualTo(before);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    /// <summary>A difference at either end keeps the replacement values isolated from caller mutations.</summary>
    /// <param name="index">The position of the differing element.</param>
    [Test]
    [Arguments(0)]
    [Arguments(2)]
    public async Task ChangedReplacementsRemainIsolated(int index)
    {
        var state = ClusterState.CreateInitial(3);
        NodeState[] nodes = [state.Nodes[0], state.Nodes[1], state.Nodes[2]];
        int[] partitions = [0, 0, 0];
        int[] match = [0, 0, 0];
        nodes[index] = nodes[index].Wiped();
        partitions[index] = 1;
        match[index] = 2;
        var changed = state.With(nodes, partitions: partitions, matchIndexes: match);

        _ = await Assert.That(changed.Nodes).IsNotSameReferenceAs(state.Nodes);
        _ = await Assert.That(changed.Partitions).IsNotSameReferenceAs(state.Partitions);
        _ = await Assert.That(changed.MatchIndexes).IsNotSameReferenceAs(state.MatchIndexes);

        nodes[index] = state.Nodes[index];
        partitions[index] = 0;
        match[index] = 0;

        _ = await Assert.That(changed.Nodes[index].Unseeded).IsTrue();
        _ = await Assert.That(changed.Partitions[index]).IsEqualTo(1);
        _ = await Assert.That(changed.MatchIndexes[index]).IsEqualTo(2);
        _ = await Assert.That(state.Nodes[index].Unseeded).IsFalse();
        _ = await Assert.That(state.Partitions[index]).IsEqualTo(0);
        _ = await Assert.That(state.MatchIndexes[index]).IsEqualTo(0);
    }

    /// <summary>Metadata patches keep an owned log, while replacement and constructor input logs are isolated from callers.</summary>
    [Test]
    public async Task NodePatchesKeepOwnedLogs()
    {
        LogEntry[] input = [new(1, 1)];
        var node = new NodeState(0, NodeRole.Follower, 1, -1, input, in NodeRuntime.Initial, false);
        var changed = ModelTransitionUtil.Patch(node, new NodePatch { ReadReady = true });
        input[0] = new LogEntry(9, 1);
        _ = await Assert.That(changed.LogEntries).IsSameReferenceAs(node.LogEntries);
        _ = await Assert.That(changed.LogEntries[0]).IsEqualTo(new LogEntry(1, 1));
        _ = await Assert.That(node.LogEntries[0]).IsEqualTo(new LogEntry(1, 1));
        _ = await Assert.That(node.ReadReady).IsFalse();
        _ = await Assert.That(changed.ReadReady).IsTrue();

        LogEntry[] replacement = [new(2, 1)];
        var replaced = ModelTransitionUtil.Patch(node, new NodePatch { LogEntries = replacement });
        replacement[0] = new LogEntry(8, 1);
        _ = await Assert.That(replaced.LogEntries).IsNotSameReferenceAs(replacement);
        _ = await Assert.That(replaced.LogEntries[0]).IsEqualTo(new LogEntry(2, 1));
        _ = await Assert.That(node.LogEntries[0]).IsEqualTo(new LogEntry(1, 1));
    }
}
