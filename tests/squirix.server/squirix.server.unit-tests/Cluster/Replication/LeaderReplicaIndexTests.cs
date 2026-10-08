using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// A leader may hold any slot of its group: its own slot counts its durable log, and every other slot is a follower that the fan-out sends
/// to and the readiness probe verifies.
/// </summary>
[Immutable]
public sealed class LeaderReplicaIndexTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Senders take the follower slots in slot order, skipping the leader slot wherever it is.</summary>
    [Test]
    public async Task SlotsSkipTheLeaderSlot()
    {
        _ = await Assert.That((new ReplicaSlots(2).SlotOf(0), new ReplicaSlots(2).SlotOf(1))).IsEqualTo((0, 1));
        _ = await Assert.That((new ReplicaSlots(1).SlotOf(0), new ReplicaSlots(1).SlotOf(1))).IsEqualTo((0, 2));
        _ = await Assert.That((new ReplicaSlots(0).SlotOf(0), new ReplicaSlots(0).SlotOf(1))).IsEqualTo((1, 2));
        _ = await Assert.That((new ReplicaSlots(1).SenderOf(0), new ReplicaSlots(1).SenderOf(2))).IsEqualTo((0, 1));
        _ = await Assert.That(new ReplicaSlots(1).IsFollower(1)).IsFalse();
    }

    /// <summary>A leader at slot 2 commits with slot 0 alone, and the fan-out never sends the entry to the leader slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderAtSlotTwoReachesMajority(CancellationToken cancellationToken)
    {
        var pipeline = new SlotPipeline(0);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 2) { LeaderReplicaIndex = 2 };
        await using var coordinator = new ReplicaCommitCoordinator(options, pipeline, ReplicaFaultHooks.CreateNoOp(), new GroupIdempotencyState(4, TimeSpan.MaxValue));

        var outcome = await coordinator.CommitAsync(ReplicaMutationTestKit.CreateMutation(), TimeSpan.FromSeconds(30), cancellationToken);

        _ = await Assert.That(outcome.Length).IsGreaterThan(0);
        _ = await Assert.That(coordinator.LeaderReplicaIndex).IsEqualTo(2);
        _ = await Assert.That(coordinator.MatchIndexFor(2)).IsEqualTo(1UL);
        _ = await Assert.That(coordinator.MatchIndexFor(0)).IsEqualTo(1UL);
        _ = await Assert.That(pipeline.SentTo.Count).IsEqualTo(2);
        foreach (var slot in pipeline.SentTo)
            _ = await Assert.That(slot).IsNotEqualTo(2);
    }

    /// <summary>The leader slot is outside the group or the options refuse it.</summary>
    [Test]
    public async Task OptionsRefuseOutsideLeaderSlot()
    {
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(3, static slot => _ = new ReplicaCommitCoordinatorOptions(3, 0, 0, 2) { LeaderReplicaIndex = slot });
        _ = await Assert.That(new ReplicaCommitCoordinatorOptions(3, 0, 0, 2).LeaderReplicaIndex).IsEqualTo(0);
    }

    /// <summary>The leader slot is marked ready from its own log, never selected for a probe, and never demoted with the followers.</summary>
    [Test]
    public async Task ProbeHelpersSkipLeaderSlot()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 4, 1, 4, 4, FollowerLogReadiness.Ready);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 1, in leader, Fingerprint, 1);
        var candidates = ReplicaReadinessProbe.NonReadyFollowers(eligibility, 1);
        ReplicaReadinessProbe.UnverifyFollowers(eligibility, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsTrue();
        _ = await Assert.That((candidates[0], candidates[1], candidates[2])).IsEqualTo((true, false, true));
    }

    /// <summary>Records the slots a commit sends to; one slot acknowledges, every other one fails.</summary>
    private sealed class SlotPipeline : IReplicaCommitPipeline
    {
        private readonly int _acknowledgingSlot;

        internal SlotPipeline(int acknowledgingSlot)
        {
            _acknowledgingSlot = acknowledgingSlot;
        }

        internal ConcurrentBag<int> SentTo { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            SentTo.Add(replicaIndex);
            return replicaIndex == _acknowledgingSlot
                ? ValueTask.FromResult(new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true))
                : ValueTask.FromException<ReplicaDurableAcknowledgement>(new TimeoutException("follower timeout"));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
