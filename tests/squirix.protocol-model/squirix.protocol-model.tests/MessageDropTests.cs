using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class MessageDropTests
{
    /// <summary>Dropping by position keeps duplicate payloads, message ids, order and the rest of the immutable snapshot.</summary>
    /// <param name="index">The position to remove.</param>
    /// <param name="firstId">The first remaining message id.</param>
    /// <param name="secondId">The second remaining message id.</param>
    [Test]
    [Arguments(0, 8, 9)]
    [Arguments(1, 7, 9)]
    [Arguments(2, 7, 8)]
    public async Task DropPreservesOrderAndOtherFields(int index, int firstId, int secondId)
    {
        var vote = MessagePayload.VoteRequest(0, 1, 2, 1, 1);
        InFlightMessage[] messages = [new(7, in vote), new(8, MessagePayload.ReadRequest(1, 2, 2, 1)), new(9, in vote)];
        var node = new NodeState(0, NodeRole.Leader, 2, 0, [new LogEntry(2, 1)], NodeRuntime.Create(1, 1, 0, 0, 0, false), false);
        var state = ClusterState.CreateInitial(3).WithDiskLoss([node, NodeState.CreateInitial(1), NodeState.CreateInitial(2).Wiped()], [1, 2, 0])
            .WithPartitions([0, 1, 0]).WithMessages(messages, 99);
        var before = state.Fingerprint(false);
        var dropped = state.WithoutMessage(index);

        _ = await Assert.That(dropped.Messages.Count).IsEqualTo(2);
        _ = await Assert.That(dropped.Messages[0]).IsEqualTo(messages[firstId - 7]);
        _ = await Assert.That(dropped.Messages[1]).IsEqualTo(messages[secondId - 7]);
        _ = await Assert.That(dropped.NextMessageId).IsEqualTo(99);
        _ = await Assert.That(dropped.Nodes).IsSameReferenceAs(state.Nodes);
        _ = await Assert.That(dropped.Partitions).IsSameReferenceAs(state.Partitions);
        _ = await Assert.That(dropped.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);
        _ = await Assert.That(dropped.CommittedEntries).IsSameReferenceAs(state.CommittedEntries);
        _ = await Assert.That(dropped.ElectedLeaders).IsSameReferenceAs(state.ElectedLeaders);
        _ = await Assert.That(dropped.DiskLosses).IsEqualTo(1);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    /// <summary>Dropping the only message leaves an empty owned collection and keeps the next id.</summary>
    [Test]
    public async Task DropOnlyMessageKeepsNextId()
    {
        var state = ClusterState.CreateInitial(3).WithMessages([new InFlightMessage(7, MessagePayload.VoteRequest(0, 1, 2, 0, 0))], 99);
        var dropped = state.WithoutMessage(0);

        _ = await Assert.That(dropped.Messages).IsSameReferenceAs(Array.Empty<InFlightMessage>());
        _ = await Assert.That(dropped.NextMessageId).IsEqualTo(99);
        _ = await Assert.That(state.Messages.Count).IsEqualTo(1);
        _ = await Assert.That(state.Messages[0].Id).IsEqualTo(7);
    }

    /// <summary>Invalid positions are rejected before constructing a replacement state.</summary>
    [Test]
    public void EmptyStateRejectsMessageIndex()
    {
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ClusterState.CreateInitial(3).WithoutMessage(-1));
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ClusterState.CreateInitial(3).WithoutMessage(0));
    }
}
