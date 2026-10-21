using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolModelEngineTests
{
    private const int ReceiverId = 0;
    private const int SenderId = 1;
    private const int ReceiverTerm = 1;
    private const int HigherTerm = 2;
    private const int FirstMessageId = 1;
    private const int ReadIndexInRound = 1;
    private const int NoVote = -1;

    private static readonly LogEntry[] SingleEntryLog = [new(1, 1)];

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
            new NodeState(0, NodeRole.Leader, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0b110, 0, 0, false), false),
            new NodeState(1, NodeRole.Follower, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(2, NodeRole.Follower, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ]);

        // Same cluster with replica ids rotated 0->1, 1->2, 2->0.
        var rotated = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Follower, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(1, NodeRole.Leader, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0b101, 0, 0, false), false),
            new NodeState(2, NodeRole.Follower, 1, 1, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ]);

        _ = await Assert.That(rotated.Fingerprint(true)).IsEqualTo(state.Fingerprint(true), StringComparer.Ordinal);
        _ = await Assert.That(rotated.Fingerprint(false)).IsNotEqualTo(state.Fingerprint(false), StringComparer.Ordinal);
    }

    [Test]
    public async Task TraceFingerprintPrefixesRawFingerprint()
    {
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Leader, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0b110, 0, 0, false), false),
            new NodeState(1, NodeRole.Follower, 1, 0, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(2, NodeRole.Follower, 1, -1, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), true),
        ]);

        const string expected = "0#0,2,1,0,1,1,6,0,0,0,0,[1@1]|0|0;1,0,1,0,1,1,0,0,0,0,0,[1@1]|0|0;2,0,1,-1,0,0,0,0,0,0,1,[]|0|0;";
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(expected, StringComparer.Ordinal);
        _ = await Assert.That(state.TraceFingerprint(12)).IsEqualTo("12:" + expected, StringComparer.Ordinal);
    }

    [Test]
    public async Task HigherTermReadReplyDeposesLeader()
    {
        var reply = MessagePayload.ReadResponse(SenderId, ReceiverId, HigherTerm, true, ReadIndexInRound);
        var delivered = DeliverSingleReply(NodeRole.Leader, ReadIndexInRound, in reply);

        await AssertSteppedDownAsync(delivered);
    }

    [Test]
    public async Task HigherTermAppendReplyStepsDown()
    {
        var reply = MessagePayload.AppendResponse(SenderId, ReceiverId, HigherTerm, 1, 1, false, 0);
        var delivered = DeliverSingleReply(NodeRole.Follower, 0, in reply);

        await AssertSteppedDownAsync(delivered);
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

    [Test]
    public async Task UnseededReplyKeepsTerm()
    {
        var append = MessagePayload.AppendResponse(SenderId, ReceiverId, HigherTerm, 1, 1, false, 0);
        var read = MessagePayload.ReadResponse(SenderId, ReceiverId, HigherTerm, true, ReadIndexInRound);

        _ = await Assert.That(DeliverToUnseeded(in append).Nodes[ReceiverId].CurrentTerm).IsEqualTo(ReceiverTerm);
        _ = await Assert.That(DeliverToUnseeded(in read).Nodes[ReceiverId].CurrentTerm).IsEqualTo(ReceiverTerm);
    }

    private static async Task AssertSteppedDownAsync(ClusterState delivered)
    {
        var receiver = delivered.Nodes[ReceiverId];
        _ = await Assert.That(receiver.CurrentTerm).IsEqualTo(HigherTerm);
        _ = await Assert.That(receiver.Role).IsEqualTo(NodeRole.Follower);
        _ = await Assert.That(receiver.VotedFor).IsEqualTo(NoVote);
        _ = await Assert.That(receiver.ReadIndex).IsEqualTo(0);
    }

    private static ClusterState DeliverToUnseeded(in MessagePayload reply)
    {
        var state = ReplyState(NodeRole.Follower, 0, true, in reply);
        var successors = ExploreRunner.NextStates(state, ExploreProfile.SmallRead(), BrokenMode.None);
        foreach (var successor in CollectionsMarshal.AsSpan(successors))
        {
            if (successor.Messages.Count == 0 && successor.Nodes[ReceiverId].Unseeded)
                return successor;
        }

        throw new InvalidOperationException("Expected a delivery successor.");
    }

    private static ClusterState ReplyState(NodeRole receiverRole, int readIndex, bool unseeded, in MessagePayload reply)
    {
        var runtime = NodeRuntime.Create(1, 1, 0, readIndex, 0, false);
        return ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(ReceiverId, receiverRole, ReceiverTerm, ReceiverId, SingleEntryLog, in runtime, unseeded),
            new NodeState(SenderId, NodeRole.Follower, HigherTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(2, NodeRole.Follower, ReceiverTerm, NoVote, SingleEntryLog, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
        ]).WithMessages([new InFlightMessage(FirstMessageId, in reply)], FirstMessageId + 1);
    }

    private static ClusterState DeliverSingleReply(NodeRole receiverRole, int readIndex, in MessagePayload reply)
    {
        var state = ReplyState(receiverRole, readIndex, false, in reply);
        var successors = ExploreRunner.NextStates(state, ExploreProfile.SmallRead(), BrokenMode.None);
        ClusterState? consumed = null;
        var consumedCount = 0;
        foreach (var successor in CollectionsMarshal.AsSpan(successors))
        {
            if (successor.Messages.Count != 0 || successor.Nodes[ReceiverId].CurrentTerm != HigherTerm)
                continue;

            consumed = successor;
            consumedCount++;
        }

        return consumedCount == 1 ? consumed! : throw new InvalidOperationException("Expected exactly one delivery successor.");
    }
}
