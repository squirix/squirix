using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A committer leading from any slot names itself as leader and sender, gets a sender for every other slot, and checks only those slots
/// before it compacts.
/// </summary>
[Immutable]
public sealed class LeaderSlotTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>The compaction check asks every follower slot but the leader's, which is not a follower of itself.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionIgnoresLeaderSlot(CancellationToken cancellationToken)
    {
        var eligibility = new ReplicaEligibility(3);
        var ready = new ReplicaProgress(1, 0, 0, 0, 0, Fingerprint, 1, 0);
        _ = eligibility.TryMarkReady(0, in ready, in ready);
        _ = eligibility.TryMarkReady(1, in ready, in ready);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 2);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue);
        await using var atTwo = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 2, 2), new IdlePipeline(), ReplicaFaultHooks.CreateNoOp(), idempotency);
        await using var atZero = new ReplicaCommitCoordinator(options, new IdlePipeline(), ReplicaFaultHooks.CreateNoOp(), idempotency);
        var clock = new FakeTimeProvider();

        var leaderAtTwo = await ReplicaLogCompactionStep.AwaitFollowersAsync(atTwo, eligibility, 0UL, clock, cancellationToken);
        var leaderAtZero = await ReplicaLogCompactionStep.AwaitFollowersAsync(atZero, eligibility, 0UL, clock, cancellationToken);

        _ = await Assert.That(leaderAtTwo).IsNull();
        _ = await Assert.That(leaderAtZero).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady);
    }

    /// <summary>A node leading a group it does not own takes its own slot and names itself, not the group, as leader and sender.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProbeHeaderNamesThisNode(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-slot-probe");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var probe = new ReplicaVerificationProbe(registry, new FixedLocator(), new IReplicaRpcGatewayCreateExpectations().Instance(), ("n1", "n2"), Fingerprint, 1, NullLogger.Instance);

        var (members, header) = probe.BuildMembership(5UL);

        _ = await Assert.That(probe.LeaderReplicaIndex).IsEqualTo(1);
        _ = await Assert.That(members[1]).IsEqualTo("n2");
        _ = await Assert.That((header.GroupId, header.LeaderNodeId, header.SenderNodeId, header.Term)).IsEqualTo(("n1", "n2", "n2", 5UL));
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            registry,
            static served => _ = new ReplicaVerificationProbe(served, new FixedLocator(), new IReplicaRpcGatewayCreateExpectations().Instance(), ("n1", "n9"), Fingerprint, 1, NullLogger.Instance));
    }

    /// <summary>The senders of a leader at slot 1 serve slots 0 and 2, in slot order.</summary>
    [Test]
    public async Task SendersServeFollowerSlots()
    {
        var header = new ReplicaRpcHeader("n1", Fingerprint, 1, 1, "n2", "n2");
        var status = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 0, 0, 0, 0, FollowerLogReadiness.Ready);
        var senders = ReplicaFollowerSenders.Create(
            new IReplicaRpcGatewayCreateExpectations().Instance(),
            ["n1", "n2", "n3"],
            1,
            in status,
            in header,
            new ReplicaFollowerSenders.SenderTiming(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), TimeProvider.System),
            static _ => { });
        try
        {
            _ = await Assert.That(senders.Length).IsEqualTo(2);
            _ = await Assert.That((senders[0].NodeId, senders[1].NodeId)).IsEqualTo(("n1", "n3"));
        }
        finally
        {
            foreach (var sender in senders)
                await sender.DisposeAsync();
        }
    }

    /// <summary>
    /// A committer of node n2 leading group n1 starts with its own slot 1 ready, commits a write with slot 2 while slot 0 is down, never
    /// sends to itself, and keeps verifying slot 0 as the one slot not ready.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitterLeadsFromSlotOne(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-slot-committer");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n1", FollowerMode.Down);
        await using var committer = new ReplicaGroupCommitter(
            registry,
            new FixedLocator(),
            gateway,
            new StubCache(),
            ("n1", "n2"),
            new ReplicaTopologyStamp(Fingerprint, 1),
            NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, "n1", "n2"),
        };

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
        var verification = await committer.VerifyReplicasAsync(cancellationToken);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That((eligibility.CanCountInWriteQuorum(0), eligibility.CanCountInWriteQuorum(1), eligibility.CanCountInWriteQuorum(2))).IsEqualTo((false, true, true));
        _ = await Assert.That(verification).IsEqualTo(ReplicaVerification.Pending);
        _ = await Assert.That(committer.Probe.LeaderReplicaIndex).IsEqualTo(1);
        var toSelf = 0;
        var toThird = 0;
        foreach (var append in gateway.Appends)
        {
            toSelf += string.Equals(append.Node, "n2", StringComparison.Ordinal) ? 1 : 0;
            toThird += string.Equals(append.Node, "n3", StringComparison.Ordinal) ? 1 : 0;
        }

        _ = await Assert.That((toSelf, toThird)).IsEqualTo((0, 1));
    }

    /// <summary>Places n1, n2 and n3 in slots 0, 1 and 2 of every group.</summary>
    private sealed class FixedLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 3;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            destination[1] = "n2";
            destination[2] = "n3";
        }
    }

    /// <summary>Commit pipeline double that is never asked to do anything by the checks under test.</summary>
    private sealed class IdlePipeline : IReplicaCommitPipeline
    {
        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException("No entry is committed by these checks."));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
