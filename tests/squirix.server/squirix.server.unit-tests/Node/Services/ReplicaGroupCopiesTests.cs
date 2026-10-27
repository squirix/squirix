using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies the status source reports the leader's view of the other copies of its group.</summary>
[Immutable]
public sealed class ReplicaGroupCopiesTests : ServerUnitTestBase
{
    /// <summary>The entries in the leader's log, all committed, when the unverified copy is listed.</summary>
    private const ulong LogLength = 5UL;

    /// <summary>
    /// Verifies the leader reads each other copy's lag from the acknowledgements of the running coordinator, so a follower that acknowledged the
    /// writes is not behind, and that its quorum follows the copies that count.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderListsCopiesWithLag(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-copies-lag");
        await SeededLogs.SeedGroupsAsync(dir, ["node-a"], cancellationToken);
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog("node-a", out var log);
        var entry = new FollowerLogEntry(1, 1, ReadOnlyMemory<byte>.Of(1, 2, 3));
        _ = await log!.AppendAsync(new FollowerLogAppendRequest("node-a", 1, 0, 0, 0, ReadOnlyMemory<FollowerLogEntry>.Of(entry)), cancellationToken);
        var eligibility = registry.EligibilityFor("node-a");
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System);

        // Node-b acknowledges the entry at once; node-c is held back, so the commit completes on the majority of the leader and node-b.
        var pipeline = new FirstFollowerPipeline();
        await using var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 2), pipeline, ReplicaFaultHooks.CreateNoOp(), idempotency, eligibility)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
        };
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), new ThreeNodeGroupLocator(), _ => coordinator, "node-a");
        var before = (await source.GetSnapshotsAsync(cancellationToken))[0];

        _ = await coordinator.CommitAsync(Mutation(), TimeSpan.FromSeconds(5), cancellationToken);
        var committed = (await source.GetSnapshotsAsync(cancellationToken))[0];
        eligibility.Forget(2);
        var mixed = (await source.GetSnapshotsAsync(cancellationToken))[0];
        eligibility.Forget(1);
        var lost = (await source.GetSnapshotsAsync(cancellationToken))[0];
        pipeline.Release();

        _ = await Assert.That((committed.ReportsCopies, committed.HasAuthority, committed.WriteQuorumAvailable)).IsEqualTo((true, true, true));
        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", "ready", 1), new ReplicaPeerStatus("node-c", "ready", 1)],
            before.Peers);
        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", "ready", 0), new ReplicaPeerStatus("node-c", "ready", 1)],
            committed.Peers);
        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", "ready", 0), new ReplicaPeerStatus("node-c", "catching-up", 1)],
            mixed.Peers);
        _ = await Assert.That(mixed.WriteQuorumAvailable).IsTrue();
        _ = await Assert.That(lost.WriteQuorumAvailable).IsFalse();
    }

    /// <summary>Verifies a copy that is not ready counts its lag from its verified report, not from the commit index the coordinator started at.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnverifiedCopyLagsByWholeLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-copies-unverified");
        await SeededLogs.SeedGroupsAsync(dir, ["node-a"], cancellationToken);
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog("node-a", out var log);
        var entries = new FollowerLogEntry[LogLength];
        for (var index = 1UL; index <= LogLength; index++)
            entries[index - 1] = new FollowerLogEntry(index, 1, ReadOnlyMemory<byte>.Of(1, 2, 3));
        _ = await log!.AppendAsync(new FollowerLogAppendRequest("node-a", 1, 0, 0, 0, ReadOnlyMemory<FollowerLogEntry>.Of(entries)), cancellationToken);
        var eligibility = registry.EligibilityFor("node-a");
        eligibility.Forget(2);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System);
        await using var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, LogLength, LogLength, 2), new FirstFollowerPipeline(), ReplicaFaultHooks.CreateNoOp(), idempotency, eligibility)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
        };
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3), new MtlsOptions(), new ThreeNodeGroupLocator(), _ => coordinator, "node-a");

        var snapshot = (await source.GetSnapshotsAsync(cancellationToken))[0];

        await SequenceAssert.EqualAsync(
            [new ReplicaPeerStatus("node-b", "ready", 0), new ReplicaPeerStatus("node-c", "catching-up", LogLength)],
            snapshot.Peers);
    }

    /// <summary>Verifies a follower lists no copies, and a leader lists them only once its coordinator runs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerListsNoCopies(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-copies-follower");
        await using var registry = CreateRegistry(dir, CreateTopology(3));
        await registry.OpenAsync(cancellationToken);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System);
        await using var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 2), new FirstFollowerPipeline(), ReplicaFaultHooks.CreateNoOp(), idempotency)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
        };
        ReplicaCommitCoordinator? running = null;
        var source = new ReplicaGroupStatusSource(registry, CreateTopology(3, true), new MtlsOptions(), new ThreeNodeGroupLocator(), _ => running, "node-a");
        var state = registry.StateFor("node-a");
        state.SetElectionDriven(true);
        state.ObserveHigherTerm(4UL);
        running = coordinator;
        var following = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.BecomeLeader(4UL);
        running = null;
        var starting = (await source.GetSnapshotsAsync(cancellationToken))[0];
        running = coordinator;
        var elected = (await source.GetSnapshotsAsync(cancellationToken))[0];
        _ = state.GrantAuthority(4UL);
        var leading = (await source.GetSnapshotsAsync(cancellationToken))[0];

        _ = await Assert.That((following.ReportsCopies, following.Peers.Count, following.HasAuthority, following.WriteQuorumAvailable)).IsEqualTo((false, 0, false, false));
        _ = await Assert.That((starting.ReportsCopies, starting.Peers.Count)).IsEqualTo((false, 0));
        _ = await Assert.That((elected.Peers.Count, elected.HasAuthority)).IsEqualTo((2, false));
        _ = await Assert.That((leading.Peers.Count, leading.HasAuthority)).IsEqualTo((2, true));
    }

    private static PreparedReplicaMutation Mutation() => new(
        new ReplicaOperationIdentity("node-a", "client", "123456789abcdef0123456789abcdef0", new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 1));

    private static ReplicaGroupRegistry CreateRegistry(string dir, TopologyOptions topology)
    {
        byte[] fingerprint = [.. TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).Bytes];
        return new ReplicaGroupRegistry(
            dir,
            ["node-a"],
            topology.ReplicaCount,
            fingerprint,
            topology.ConfigurationGeneration,
            NullLoggerFactory.Instance,
            new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System })
        {
            ElectionClock = TimeProvider.System,
        };
    }

    private static TopologyOptions CreateTopology(int replicaCount, bool automaticFailover = false) => new([new ServerPeer { NodeId = "node-a", Uri = new Uri("https://localhost:6131") }])
    {
        ClusterId = "copies-c",
        NodeId = "node-a",
        Uri = new Uri("https://localhost:6131"),
        ReplicaCount = replicaCount,
        AutomaticFailoverEnabled = automaticFailover,
    };

    /// <summary>Commit pipeline whose second slot acknowledges every entry at once and whose other followers answer only once released.</summary>
    private sealed class FirstFollowerPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public async ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (replicaIndex != 1)
                await _released.Task.ConfigureAwait(false);

            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        }

        public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void Release() => _ = _released.TrySetResult();
    }
}
