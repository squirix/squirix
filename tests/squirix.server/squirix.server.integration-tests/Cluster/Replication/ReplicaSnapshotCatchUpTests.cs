using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Snapshot fallback, tail replay, and final eligibility verification.</summary>
public sealed class ReplicaSnapshotCatchUpTests : NodeIntegrationTestBase
{
    private const string GroupId = "snapshot-catch-up";
    private static readonly byte[] Fingerprint = [4, 8, 15, 16, 23, 42];

    /// <summary>A transfer checksum mismatch quarantines the participant before publication or replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChecksumMismatchQuarantinesReplica(CancellationToken cancellationToken)
    {
        using var targetDir = new TempDirectory("squirix-catch-up-mismatch");
        var transfer = ReplicaSnapshotTransfer.Create(Snapshot()) with { PayloadChecksum = 0U };
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        var eligibility = new ReplicaEligibility(3);
        var session = new ReplicaSnapshotCatchUpSession(new ReplicaRepairPlanner(2), follower, eligibility, 1);
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = Expected(73U),
            FinalizeStateAsync = static _ => ValueTask.FromResult(73U),
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = new[] { Entry(3UL, 2UL, "three"), Entry(4UL, 2UL, "four") },
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.Quarantined);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
        _ = await Assert.That(follower.SnapshotPath).IsNull();
    }

    /// <summary>A follower behind compaction installs the latest published baseline, replays the tail, and becomes ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedFollowerCatchesUp(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-catch-up-source");
        using var targetDir = new TempDirectory("squirix-catch-up-target");
        var snapshot = Snapshot();
        await using var source = new FollowerLog(sourceDir, GroupId, GroupComposition.Create(GroupId));
        await source.OpenAsync(cancellationToken);
        var store = new GroupSnapshotStore(sourceDir, GroupId);
        await store.PublishAsync(snapshot, cancellationToken);
        var published = await Assert.That(await store.ReadPublishedAsync(cancellationToken)).IsTypeOf<GroupSnapshot>();
        var transfer = ReplicaSnapshotTransfer.Create(in published);
        var tail = new[] { Entry(3UL, 2UL, "three"), Entry(4UL, 2UL, "four") };
        var planner = new ReplicaRepairPlanner(1);

        var snapshotSelection = planner.SelectRepair(tail, 1UL, transfer);
        var entrySelection = planner.SelectRepair(tail, 3UL, transfer);
        _ = await Assert.That(snapshotSelection.Kind).IsEqualTo(ReplicaRepairSelectionKind.Snapshot);
        _ = await Assert.That(entrySelection.Kind).IsEqualTo(ReplicaRepairSelectionKind.Entries);
        _ = await Assert.That(GroupSnapshotStore.ComputePayloadIntegrity(published).Length).IsEqualTo(transfer.PayloadLength);
        _ = await Assert.That(GroupSnapshotStore.ComputePayloadIntegrity(published).Checksum).IsEqualTo(transfer.PayloadChecksum);

        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        var eligibility = new ReplicaEligibility(3);
        var expected = Expected(73U);
        var session = new ReplicaSnapshotCatchUpSession(planner, follower, eligibility, 1);
        var observedCatchingUp = false;
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = expected,
            FinalizeStateAsync = _ =>
            {
                observedCatchingUp = eligibility.StateFor(1) == ReplicaParticipantState.CatchingUp;
                return ValueTask.FromResult(73U);
            },
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = tail,
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsTrue();
        _ = await Assert.That(observedCatchingUp).IsTrue();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsTrue();
        _ = await Assert.That(eligibility.ProgressFor(1).Matches(in expected)).IsTrue();
        var status = await follower.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(4UL);
        _ = await Assert.That(status.LastLogTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(4UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(4UL);
    }

    /// <summary>A tail the leader no longer retains fails the session without quarantine.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedTailFailsWithoutQuarantine(CancellationToken cancellationToken)
    {
        using var targetDir = new TempDirectory("squirix-catch-up-compacted");
        var transfer = ReplicaSnapshotTransfer.Create(Snapshot());
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        var eligibility = new ReplicaEligibility(3);
        var session = new ReplicaSnapshotCatchUpSession(new ReplicaRepairPlanner(2), follower, eligibility, 1);
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = Expected(73U),
            FinalizeStateAsync = static _ => ValueTask.FromResult(73U),
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = new[] { Entry(3UL, 2UL, "three") },
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
    }

    /// <summary>A snapshot from a conflicting durable topology quarantines the participant.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConflictingTopologyQuarantinesReplica(CancellationToken cancellationToken)
    {
        using var targetDir = new TempDirectory("squirix-catch-up-topology");
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        var installed = await follower.InstallSnapshotAsync(SnapshotWith([7]), 1UL, cancellationToken);
        _ = await Assert.That(installed.Success).IsTrue();

        var transfer = ReplicaSnapshotTransfer.Create(SnapshotWith([8]));
        var eligibility = new ReplicaEligibility(3);
        var session = new ReplicaSnapshotCatchUpSession(new ReplicaRepairPlanner(2), follower, eligibility, 1);
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = Expected(73U) with { TopologyFingerprint = new byte[] { 8 } },
            FinalizeStateAsync = static _ => ValueTask.FromResult(73U),
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = new[] { Entry(3UL, 2UL, "three"), Entry(4UL, 2UL, "four") },
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.Quarantined);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
    }

    /// <summary>A snapshot from a deposed leader is refused without quarantine and without durable writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleLeaderTermRefusedWithoutQuarantine(CancellationToken cancellationToken)
    {
        using var targetDir = new TempDirectory("squirix-catch-up-stale-term");
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        _ = await Assert.That((await follower.AppendAsync(Append(1UL, 3UL, "one"), cancellationToken)).Success).IsTrue();

        var transfer = ReplicaSnapshotTransfer.Create(Snapshot());
        var eligibility = new ReplicaEligibility(3);
        var session = new ReplicaSnapshotCatchUpSession(new ReplicaRepairPlanner(2), follower, eligibility, 1);
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = Expected(73U),
            FinalizeStateAsync = static _ => ValueTask.FromResult(73U),
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = Array.Empty<FollowerLogEntry>(),
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
        _ = await Assert.That(follower.SnapshotPath).IsNull();
        _ = await Assert.That((await follower.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(3UL);
    }

    /// <summary>A snapshot below the durable commit watermark is refused without quarantine.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleSnapshotRefusedWithoutQuarantine(CancellationToken cancellationToken)
    {
        using var targetDir = new TempDirectory("squirix-catch-up-stale");
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(cancellationToken);
        _ = await Assert.That((await follower.AppendAsync(Append(1UL, 1UL, "one"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await follower.AppendAsync(Append(2UL, 1UL, "two"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await follower.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();

        var stale = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        var transfer = ReplicaSnapshotTransfer.Create(in stale);
        var eligibility = new ReplicaEligibility(3);
        var session = new ReplicaSnapshotCatchUpSession(new ReplicaRepairPlanner(2), follower, eligibility, 1);
        var request = new ReplicaSnapshotCatchUpRequest
        {
            Expected = Expected(73U),
            FinalizeStateAsync = static _ => ValueTask.FromResult(73U),
            LeaderNodeId = "leader-1",
            LeaderTerm = 2UL,
            Snapshot = transfer,
            TailEntries = new[] { Entry(3UL, 2UL, "three"), Entry(4UL, 2UL, "four") },
        };

        _ = await Assert.That(await session.RunAsync(request, cancellationToken)).IsFalse();
        _ = await Assert.That(eligibility.StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That(eligibility.CanCountInWriteQuorum(1)).IsFalse();
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader",
        term,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([Entry(index, term, payload)]));

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));

    private static ReplicaProgress Expected(uint checksum) => new(5UL, 4UL, 4UL, 4UL, 2UL, Fingerprint, 1UL, checksum);

    private static GroupSnapshot Snapshot() => new(GroupId, Fingerprint, 1UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());

    private static GroupSnapshot SnapshotWith(byte[] fingerprint) => new(GroupId, fingerprint, 1UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
}
