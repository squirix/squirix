using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class MessageDeliveryTests
{
    [Test]
    [Arguments(1, 0)]
    [Arguments(2, 1)]
    public async Task AppendAckKeepsCurrentTermRule(int entryTerm, int expectedCommit)
    {
        var nodes = CreateNodes(3);
        nodes[0] = new NodeState(0, NodeRole.Leader, 2, 0, [new LogEntry(entryTerm, 1)], in NodeRuntime.Initial, false);
        nodes[1] = new NodeState(1, NodeRole.Follower, 2, -1, [new LogEntry(entryTerm, 1)], in NodeRuntime.Initial, false);
        var state = CreateState(nodes, MessagePayload.AppendResponse(1, 0, 2, 1, entryTerm, true, 1));
        var next = FindDelivery(state, Profile(3, 1, 4), static candidate => candidate.MatchIndexes[1] == 1);

        _ = await Assert.That(next.MatchIndexes[0]).IsEqualTo(1);
        _ = await Assert.That(next.MatchIndexes[1]).IsEqualTo(1);
        _ = await Assert.That(next.MatchIndexes[2]).IsEqualTo(0);
        _ = await Assert.That(next.Nodes[0].CommitIndex).IsEqualTo(expectedCommit);
        _ = await Assert.That(next.NextMessageId).IsEqualTo(100);
        _ = await Assert.That(state.Nodes[0].CommitIndex).IsEqualTo(0);
        _ = await Assert.That(state.MatchIndexes[1]).IsEqualTo(0);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task HigherTermReplyKeepsOrder(int kind)
    {
        var nodes = CreateNodes(3);
        nodes[0] = new NodeState(0, NodeRole.Leader, 2, 0, [new LogEntry(2, 1)], NodeRuntime.Create(1, 1, 3, 1, 1, true), false);
        var state = CreateState(nodes, Reply(kind, 3));
        var before = state.Fingerprint(false);
        var next = FindDelivery(state, Profile(3, 1, 4), static candidate => candidate.Nodes[0].CurrentTerm == 3);

        _ = await Assert.That(next.Nodes[0].Role).IsEqualTo(NodeRole.Follower);
        _ = await Assert.That(next.Nodes[0].VotedFor).IsEqualTo(-1);
        _ = await Assert.That(next.Nodes[0].VotesGranted).IsEqualTo(0);
        _ = await Assert.That(next.Nodes[0].ReadIndex).IsEqualTo(0);
        _ = await Assert.That(next.Nodes[0].ReadAcks).IsEqualTo(0);
        _ = await Assert.That(next.Nodes[0].ReadReady).IsFalse();
        _ = await Assert.That(next.Nodes[0].LogEntries).IsSameReferenceAs(state.Nodes[0].LogEntries);
        _ = await Assert.That(next.Nodes[0].CommitIndex).IsEqualTo(1);
        _ = await Assert.That(next.Nodes[0].AppliedIndex).IsEqualTo(1);
        _ = await Assert.That(next.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);
        _ = await Assert.That(next.NextMessageId).IsEqualTo(100);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    [Test]
    [Arguments(0, false)]
    [Arguments(1, false)]
    [Arguments(2, false)]
    [Arguments(3, false)]
    [Arguments(0, true)]
    [Arguments(1, true)]
    [Arguments(2, true)]
    [Arguments(3, true)]
    public async Task IgnoredDeliveryKeepsBothSuccessors(int kind, bool unseeded)
    {
        var nodes = CreateNodes(3);
        if (unseeded)
            nodes[0] = nodes[0].Wiped();

        var state = CreateState(nodes, Reply(kind, unseeded ? 3 : 1));
        var before = state.Fingerprint(false);
        var successors = ExploreRunner.NextStates(state, Profile(3, 1, 4), BrokenMode.None);
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            var next = successors[i];
            if (!ConsumedMiddle(next))
                continue;

            count++;
            _ = await Assert.That(next.Nodes).IsSameReferenceAs(state.Nodes);
            _ = await Assert.That(next.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);
            _ = await Assert.That(next.NextMessageId).IsEqualTo(100);
        }

        // Delivery and network loss remain separate transitions, even when their results coincide.
        _ = await Assert.That(count).IsEqualTo(2);
        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    [Test]
    public async Task NonWinningVoteOnlyRecordsVote()
    {
        var nodes = CreateNodes(5);
        nodes[0] = new NodeState(0, NodeRole.Candidate, 2, 0, [], NodeRuntime.Create(0, 0, 1, 0, 0, false), false);
        var state = CreateState(nodes, MessagePayload.VoteResponse(1, 0, 2, true));
        var next = FindDelivery(state, Profile(5, 1, 6), static candidate => candidate.Nodes[0].VotesGranted == 3);

        _ = await Assert.That(next.Nodes[0].Role).IsEqualTo(NodeRole.Candidate);
        _ = await Assert.That(next.Nodes[0].LastLogIndex).IsEqualTo(0);
        _ = await Assert.That(next.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);
        _ = await Assert.That(next.NextMessageId).IsEqualTo(100);
        _ = await Assert.That(state.Nodes[0].VotesGranted).IsEqualTo(1);
    }

    [Test]
    public async Task ReadAckCompletesWithoutReply()
    {
        var nodes = CreateNodes(3);
        nodes[0] = new NodeState(0, NodeRole.Leader, 2, 0, [new LogEntry(2, 1)], NodeRuntime.Create(1, 1, 0, 1, 1, false), false);
        var state = CreateState(nodes, MessagePayload.ReadResponse(1, 0, 2, true, 1));
        var next = FindDelivery(state, Profile(3, 1, 4), static candidate => candidate.Nodes[0].ReadReady);

        _ = await Assert.That(next.Nodes[0].ReadAcks).IsEqualTo(3);
        _ = await Assert.That(next.Nodes[0].ReadIndex).IsEqualTo(1);
        _ = await Assert.That(next.MatchIndexes).IsSameReferenceAs(state.MatchIndexes);
        _ = await Assert.That(next.NextMessageId).IsEqualTo(100);
        _ = await Assert.That(state.Nodes[0].ReadReady).IsFalse();
        _ = await Assert.That(state.Messages.Count).IsEqualTo(3);
    }

    [Test]
    public async Task RemovalOwnsReplacementCollections()
    {
        var state = CreateState(CreateNodes(3), Reply(0, 1));
        var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
        var changed = ModelTransitionUtil.Patch(nodes[0], new NodePatch { CurrentTerm = 3 });
        nodes[0] = changed;
        int[] match = [1, 0, 0];
        var next = state.WithoutMessage(1, nodes, match);
        Array.Fill(nodes, NodeState.CreateInitial(0));
        match[0] = 99;

        _ = await Assert.That(next.Nodes[0]).IsSameReferenceAs(changed);
        _ = await Assert.That(next.MatchIndexes[0]).IsEqualTo(1);
        _ = await Assert.That(next.Messages[0]).IsEqualTo(state.Messages[0]);
        _ = await Assert.That(next.Messages[1]).IsEqualTo(state.Messages[2]);
        _ = await Assert.That(state.Nodes[0].CurrentTerm).IsEqualTo(2);
        _ = await Assert.That(state.MatchIndexes[0]).IsEqualTo(0);
    }

    [Test]
    [Arguments(2, 3, 1)]
    [Arguments(3, 4, 1)]
    [Arguments(5, 5, 1)]
    [Arguments(5, 6, 1)]
    [Arguments(5, 7, 1)]
    [Arguments(32, 32, 1)]
    [Arguments(32, 33, 1)]
    [Arguments(5, 6, 0)]
    public async Task WinningVotePreservesFanOut(int replicas, int maxInFlight, int maxLog)
    {
        var nodes = CreateNodes(replicas);
        var lastVoter = replicas / 2;
        var votes = (1 << lastVoter) - 1;
        nodes[0] = new NodeState(0, NodeRole.Candidate, 2, 0, [], NodeRuntime.Create(0, 0, votes, 0, 0, false), false);
        var state = CreateState(nodes, MessagePayload.VoteResponse(lastVoter, 0, 2, true));
        var before = state.Fingerprint(false);
        var successors = ExploreRunner.NextStates(state, Profile(replicas, maxLog, maxInFlight), BrokenMode.None);
        ClusterState? promoted = null;
        for (var i = 0; i < successors.Count; i++)
        {
            if (successors[i].Nodes[0].Role != NodeRole.Leader)
                continue;

            _ = await Assert.That(promoted).IsNull();
            promoted = successors[i];
        }

        _ = await Assert.That(promoted).IsNotNull();
        var next = promoted!;
        var sends = maxLog > 0 && replicas + 1 <= maxInFlight ? replicas - 1 : 0;
        _ = await Assert.That(next.Messages.Count).IsEqualTo(2 + sends);
        _ = await Assert.That(next.Messages[0]).IsEqualTo(state.Messages[0]);
        _ = await Assert.That(next.Messages[1]).IsEqualTo(state.Messages[2]);
        _ = await Assert.That(next.NextMessageId).IsEqualTo(100 + sends);
        _ = await Assert.That(next.Nodes[0].VotesGranted).IsEqualTo(votes | (1 << lastVoter));
        _ = await Assert.That(next.Nodes[0].LastLogIndex).IsEqualTo(maxLog);
        _ = await Assert.That(next.MatchIndexes[0]).IsEqualTo(maxLog);
        for (var peer = 1; peer < replicas; peer++)
            _ = await Assert.That(next.MatchIndexes[peer]).IsEqualTo(0);

        for (var i = 0; i < sends; i++)
        {
            var expected = new InFlightMessage(100 + i, MessagePayload.Append((0, i + 1, 2), new LogEntry(2, 1), 0));
            _ = await Assert.That(next.Messages[i + 2]).IsEqualTo(expected);
        }

        _ = await Assert.That(state.Fingerprint(false)).IsEqualTo(before);
    }

    private static bool ConsumedMiddle(ClusterState state) => state.Messages is [{ Id: 10 } _, { Id: 30 } _];

    private static NodeState[] CreateNodes(int count)
    {
        var nodes = new NodeState[count];
        for (var i = 0; i < count; i++)
            nodes[i] = new NodeState(i, NodeRole.Follower, 2, -1, [], in NodeRuntime.Initial, false);

        return nodes;
    }

    private static ClusterState CreateState(NodeState[] nodes, in MessagePayload reply) => ClusterState.CreateInitial(nodes.Length)
                                                                                                       .WithNodes(nodes).WithMessages(
                                                                                                            [
                                                                                                                new InFlightMessage(10, MessagePayload.TransferHint(1, 0, 1)),
                                                                                                                new InFlightMessage(20, in reply),
                                                                                                                new InFlightMessage(30, MessagePayload.TransferHint(1, 0, 1)),
                                                                                                            ],
                                                                                                            100);

    private static ClusterState FindDelivery(ClusterState state, ExploreProfile profile, Func<ClusterState, bool> predicate)
    {
        var successors = ExploreRunner.NextStates(state, profile, BrokenMode.None);
        ClusterState? delivered = null;
        var count = 0;
        for (var i = 0; i < successors.Count; i++)
        {
            var candidate = successors[i];
            if (!ConsumedMiddle(candidate) || !predicate(candidate))
                continue;

            count++;
            delivered = candidate;
        }

        return count == 1 ? delivered! : throw new InvalidOperationException("Expected exactly one delivery.");
    }

    private static ExploreProfile Profile(int replicas, int maxLog, int maxInFlight) => ExploreProfile.ForReplicaCount(replicas, 2, maxLog, maxInFlight, 0, false, false);

    private static MessagePayload Reply(int kind, int term) => kind switch
    {
        0 => MessagePayload.AppendResponse(1, 0, term, 0, 0, false, 0),
        1 => MessagePayload.ReadResponse(1, 0, term, false, 0),
        2 => MessagePayload.VoteResponse(1, 0, term, false),
        3 => MessagePayload.TransferHint(1, 0, term),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };
}
