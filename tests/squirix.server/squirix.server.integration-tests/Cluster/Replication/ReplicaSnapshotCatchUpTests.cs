using System;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Snapshot fallback, tail replay, and final eligibility verification.</summary>
public sealed class ReplicaSnapshotCatchUpTests : NodeIntegrationTestBase
{
    private const string GroupId = "snapshot-catch-up";
    private static readonly byte[] Fingerprint = [4, 8, 15, 16, 23, 42];

    /// <summary>A follower behind compaction installs the latest published baseline, replays the tail, and becomes ready.</summary>
    [Fact(DisplayName = "Squirix.Server.IntegrationTests.Cluster.Replication.ReplicaSnapshotCatchUpTests.CompactedFollowerInstallsSnapshotThenEntries")]
    public async Task CompactedFollowerCatchesUp()
    {
        using var sourceDir = new TempDirectory("squirix-catch-up-source");
        using var targetDir = new TempDirectory("squirix-catch-up-target");
        var snapshot = Snapshot();
        await using var source = new FollowerLog(sourceDir, GroupId, GroupComposition.Create(GroupId));
        await source.OpenAsync(DefaultCancellationToken);
        var store = new GroupSnapshotStore(sourceDir, GroupId);
        await store.PublishAsync(snapshot, DefaultCancellationToken);
        var published = Assert.IsType<GroupSnapshot>(await store.ReadPublishedAsync(DefaultCancellationToken));
        var transfer = ReplicaSnapshotTransfer.Create(in published);
        var tail = new[] { Entry(3UL, 2UL, "three"), Entry(4UL, 2UL, "four") };
        var planner = new ReplicaRepairPlanner(1);

        var snapshotSelection = planner.SelectRepair(tail, 1UL, transfer);
        var entrySelection = planner.SelectRepair(tail, 3UL, transfer);
        Assert.Equal(ReplicaRepairSelectionKind.Snapshot, snapshotSelection.Kind);
        Assert.Equal(ReplicaRepairSelectionKind.Entries, entrySelection.Kind);
        Assert.Equal(transfer.PayloadLength, GroupSnapshotStore.ComputePayloadIntegrity(published).Length);
        Assert.Equal(transfer.PayloadChecksum, GroupSnapshotStore.ComputePayloadIntegrity(published).Checksum);

        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(DefaultCancellationToken);
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

        Assert.True(await session.RunAsync(request, DefaultCancellationToken));
        Assert.True(observedCatchingUp);
        Assert.Equal(ReplicaParticipantState.Ready, eligibility.StateFor(1));
        Assert.True(eligibility.CanCountInWriteQuorum(1));
        Assert.True(eligibility.ProgressFor(1).Matches(in expected));
        var status = await follower.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(4UL, status.LastLogIndex);
        Assert.Equal(2UL, status.LastLogTerm);
        Assert.Equal(4UL, status.CommitIndex);
        Assert.Equal(4UL, status.LastAppliedIndex);
    }

    /// <summary>A transfer checksum mismatch quarantines the participant before publication or replay.</summary>
    [Fact]
    public async Task ChecksumMismatchQuarantinesReplica()
    {
        using var targetDir = new TempDirectory("squirix-catch-up-mismatch");
        var transfer = ReplicaSnapshotTransfer.Create(Snapshot()) with { PayloadChecksum = 0U };
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(DefaultCancellationToken);
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

        Assert.False(await session.RunAsync(request, DefaultCancellationToken));
        Assert.Equal(ReplicaParticipantState.Quarantined, eligibility.StateFor(1));
        Assert.False(eligibility.CanCountInWriteQuorum(1));
        Assert.Null(follower.SnapshotPath);
    }

    /// <summary>A tail the leader no longer retains fails the session without quarantine.</summary>
    [Fact]
    public async Task CompactedTailFailsWithoutQuarantine()
    {
        using var targetDir = new TempDirectory("squirix-catch-up-compacted");
        var transfer = ReplicaSnapshotTransfer.Create(Snapshot());
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(DefaultCancellationToken);
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

        Assert.False(await session.RunAsync(request, DefaultCancellationToken));
        Assert.Equal(ReplicaParticipantState.CatchingUp, eligibility.StateFor(1));
        Assert.False(eligibility.CanCountInWriteQuorum(1));
    }

    /// <summary>A snapshot from a conflicting durable topology quarantines the participant.</summary>
    [Fact]
    public async Task ConflictingTopologyQuarantinesReplica()
    {
        using var targetDir = new TempDirectory("squirix-catch-up-topology");
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(DefaultCancellationToken);
        var installed = await follower.InstallSnapshotAsync(SnapshotWith([7]), DefaultCancellationToken);
        Assert.True(installed.Success);

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

        Assert.False(await session.RunAsync(request, DefaultCancellationToken));
        Assert.Equal(ReplicaParticipantState.Quarantined, eligibility.StateFor(1));
        Assert.False(eligibility.CanCountInWriteQuorum(1));
    }

    /// <summary>A snapshot below the durable commit watermark is refused without quarantine.</summary>
    [Fact]
    public async Task StaleSnapshotRefusedWithoutQuarantine()
    {
        using var targetDir = new TempDirectory("squirix-catch-up-stale");
        await using var follower = new FollowerLog(targetDir, GroupId, GroupComposition.Create(GroupId));
        await follower.OpenAsync(DefaultCancellationToken);
        Assert.True((await follower.AppendAsync(Append(1UL, 1UL, "one"), DefaultCancellationToken)).Success);
        Assert.True((await follower.AppendAsync(Append(2UL, 1UL, "two"), DefaultCancellationToken)).Success);
        Assert.True((await follower.AdvanceCommitAsync(2UL, DefaultCancellationToken)).Success);

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

        Assert.False(await session.RunAsync(request, DefaultCancellationToken));
        Assert.Equal(ReplicaParticipantState.CatchingUp, eligibility.StateFor(1));
        Assert.False(eligibility.CanCountInWriteQuorum(1));
    }

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader",
        term,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([Entry(index, term, payload)]));

    private static GroupSnapshot SnapshotWith(byte[] fingerprint) => new(GroupId, fingerprint, 1UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());

    private static ReplicaProgress Expected(uint checksum) => new(5UL, 4UL, 4UL, 4UL, 2UL, Fingerprint, 1UL, checksum);

    private static GroupSnapshot Snapshot() => new(GroupId, Fingerprint, 1UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
}
