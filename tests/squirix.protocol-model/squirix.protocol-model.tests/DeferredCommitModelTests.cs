using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Models the durable leadership barrier and subsequent deferred commit checkpoints.</summary>
public sealed class DeferredCommitModelTests
{
    private static readonly ExploreProfile Profile = ExploreProfile.ForReplicaCount(3, 4, 4, 12, 1, true, false);

    /// <summary>The first commit of an elected term and commits in term one remain synchronous.</summary>
    /// <param name="term">The leader term.</param>
    /// <param name="priorCommit">The index committed before the acknowledgement arrives.</param>
    /// <param name="checkpoint">The expected checkpoint after the acknowledgement.</param>
    [Test]
    [Arguments(1, 1, 2)]
    [Arguments(2, 0, 2)]
    [Arguments(2, 1, 1)]
    public async Task OnlyAuthorizedTermDefers(int term, int priorCommit, int checkpoint)
    {
        var committed = CommitSecond(term, priorCommit);
        _ = await Assert.That(committed.Nodes[0].DurableCommitIndex).IsEqualTo(checkpoint);
    }

    /// <summary>An acknowledged entry survives a crash before its checkpoint, and maintenance can persist the index.</summary>
    [Test]
    public async Task CrashKeepsWalAndCommitEvidence()
    {
        var committed = CommitSecond(2, 1);
        var applied = ExploreRunner.NextStates(committed, Profile, BrokenMode.None).Find(static s => s.Nodes[0].AppliedIndex == 2)!;
        var next = ExploreRunner.NextStates(applied, Profile, BrokenMode.None);
        var crashed = next.Find(static s => s.Nodes[0] is { Role: NodeRole.Follower, CurrentTerm: 2, CommitIndex: 1 })!;
        _ = await Assert.That((crashed.Nodes[0].LastLogIndex, crashed.Nodes[0].AppliedIndex)).IsEqualTo((2, 0));
        var retained = false;
        foreach (var entry in crashed.CommittedEntries)
            retained |= entry is { Index: 2, EntryTerm: 2 };

        _ = await Assert.That(retained).IsTrue();
        _ = await Assert.That(SafetyChecker.Check(crashed)).IsNull();
        _ = await Assert.That(next.Exists(static s => s.Nodes[0] is { Role: NodeRole.Leader, CommitIndex: 2, DurableCommitIndex: 2 })).IsTrue();
    }

    /// <summary>A newer leader cannot start a read until its own term commits through the retained tail.</summary>
    [Test]
    public async Task ReadWaitsForNewTermBarrier()
    {
        var state = CommitSecond(2, 1);
        var log = new[] { new LogEntry(2, 1), new LogEntry(2, 2), new LogEntry(3, 3) };
        var leader = new NodeState(0, NodeRole.Leader, 3, 0, log, NodeRuntime.Create(1, 1, 3, 0, 0, false), false);
        var follower = new NodeState(1, NodeRole.Follower, 3, 0, log, NodeRuntime.Create(1, 1, 0, 0, 0, false), false);
        var elected = state.WithNodes([leader, follower, state.Nodes[2]]);
        _ = await Assert.That(ExploreRunner.NextStates(elected, Profile, BrokenMode.None).Exists(static s => s.Nodes[0].ReadIndex != 0)).IsFalse();
        var ack = MessagePayload.AppendResponse(1, 0, 3, 3, 3, true, 3);
        var ready = ExploreRunner.NextStates(elected.WithMessages([new InFlightMessage(1, in ack)], 2), Profile, BrokenMode.None)
                                 .Find(static s => s.Nodes[0].CommitIndex == 3)!;
        _ = await Assert.That(ready.Nodes[0].DurableCommitIndex).IsEqualTo(3);
        _ = await Assert.That(ExploreRunner.NextStates(ready, Profile, BrokenMode.None).Exists(static s => s.Nodes[0].ReadIndex == 3)).IsTrue();
    }

    private static ClusterState CommitSecond(int term, int priorCommit)
    {
        LogEntry[] log = [new(term, 1), new(term, 2)];
        var ack = MessagePayload.AppendResponse(1, 0, term, 2, term, true, 2);
        var state = ClusterState.CreateInitial(3).WithNodes(
        [
            new NodeState(0, NodeRole.Leader, term, 0, log, NodeRuntime.Create(priorCommit, priorCommit, 3, 0, 0, false), false),
            new NodeState(1, NodeRole.Follower, term, 0, log, in NodeRuntime.Initial, false),
            NodeState.CreateInitial(2),
        ]).WithMessages([new InFlightMessage(1, in ack)], 2);
        return ExploreRunner.NextStates(state, Profile, BrokenMode.None).Find(static s => s.Nodes[0].CommitIndex == 2)!;
    }
}
