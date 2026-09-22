using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Follower replication over real group logs: append, commit, status, and snapshot install.</summary>
public sealed class ReplicaFollowerTests : NodeIntegrationTestBase
{
    private const string GroupId = "follower-service";
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Commit advances within the durable prefix and refuses beyond it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceCommitAcceptedWithinPrefix(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-commit");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), cancellationToken);

        var advanced = await service.AdvanceCommitAsync(GroupId, Fingerprint, 1UL, 2UL, 1UL, cancellationToken);

        _ = await Assert.That(advanced.Success).IsTrue();
        _ = await Assert.That(advanced.CommitIndex).IsEqualTo(2UL);

        var beyond = await service.AdvanceCommitAsync(GroupId, Fingerprint, 1UL, 5UL, 1UL, cancellationToken);

        _ = await Assert.That(beyond.Success).IsFalse();
        _ = await Assert.That(beyond.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
    }

    /// <summary>Advancing an unserved group is refused without touching storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceCommitUnknownGroupRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-commit-unknown");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        var result = await service.AdvanceCommitAsync("missing", Fingerprint, 1UL, 1UL, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Accepted appends advance the durable log and are visible in status.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendAcceptedAdvancesLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-append");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(result.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(result.LastLogIndex).IsEqualTo(2UL);
        var live = await service.GetStatusAsync(GroupId, cancellationToken);
        var current = await Assert.That(live).IsNotNull();
        _ = await Assert.That(current.LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>An append from a deposed leader term is refused without touching the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendStaleTermRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-stale");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 2UL, 0UL, 0UL, 0UL, Record(1UL, 2UL)), cancellationToken);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 1UL, 2UL, 0UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(result.CurrentTerm).IsEqualTo(2UL);
        var stale = await service.GetStatusAsync(GroupId, cancellationToken);
        var staleStatus = await Assert.That(stale).IsNotNull();
        _ = await Assert.That(staleStatus.LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>Appending to an unserved group is refused without touching storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendToUnknownGroupRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-unknown");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        var result = await service.AppendAsync("missing", Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Conflicting fingerprints and older generations are refused as topology mismatches.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendTopologyMismatchRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-topology");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        _ = await Assert.That((await service.InstallSnapshotAsync(GroupId, Fingerprint, 1UL, snapshot, 1UL, cancellationToken)).Success).IsTrue();

        var fingerprint = await service.AppendAsync(GroupId, new byte[] { 4 }, 1UL, Batch("leader", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(fingerprint.Success).IsFalse();
        _ = await Assert.That(fingerprint.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);

        var generation = await service.AppendAsync(GroupId, Fingerprint, 0UL, Batch("leader", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), cancellationToken);

        _ = await Assert.That(generation.Success).IsFalse();
        _ = await Assert.That(generation.RefusalCode).IsEqualTo(FollowerLogRefusal.TopologyMismatch);
    }

    /// <summary>A transferred snapshot file installs through validation into the group log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallSnapshotUploadAccepted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-upload");
        using var dir2 = new TempDirectory("squirix-follower-upload-source");
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        var fileBytes = await PublishAsync(dir2, snapshot, cancellationToken);
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        var result = await service.InstallSnapshotUploadAsync(GroupId, Fingerprint, 1UL, Upload(fileBytes, snapshot), 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        var installed = await service.GetStatusAsync(GroupId, cancellationToken);
        var installedStatus = await Assert.That(installed).IsNotNull();
        _ = await Assert.That(installedStatus.LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(installedStatus.CommitIndex).IsEqualTo(1UL);
    }

    /// <summary>A corrupted snapshot transfer is refused without touching storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallSnapshotUploadCorruptRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-upload-corrupt");
        using var dir2 = new TempDirectory("squirix-follower-upload-corrupt-source");
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        var fileBytes = await PublishAsync(dir2, snapshot, cancellationToken);
        fileBytes[^1] ^= 0xFF;
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        var result = await service.InstallSnapshotUploadAsync(GroupId, Fingerprint, 1UL, Upload(fileBytes, snapshot), 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        var rejected = await service.GetStatusAsync(GroupId, cancellationToken);
        var rejectedStatus = await Assert.That(rejected).IsNotNull();
        _ = await Assert.That(rejectedStatus.LastLogIndex).IsEqualTo(0UL);
    }

    /// <summary>Codec round-trips every record field and rejects truncation and trailing bytes.</summary>
    [Test]
    public async Task LogCodecRoundTripsRecord()
    {
        var record = Record(7UL, 3UL);
        var bytes = ReplicaLogCodec.Encode(in record);

        var decoded = await Assert.That(ReplicaLogCodec.Decode(bytes)).IsNotNull();
        await SequenceAssert.EqualAsync(bytes, ReplicaLogCodec.Encode(in decoded));
        _ = await Assert.That(ReplicaLogCodec.Decode(bytes.AsMemory(0, bytes.Length - 1))).IsNull();

        var trailed = new byte[bytes.Length + 1];
        bytes.CopyTo(trailed, 0);
        _ = await Assert.That(ReplicaLogCodec.Decode(trailed)).IsNull();
    }

    /// <summary>Status of an unserved group is null.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusUnknownGroupIsNull(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-status-unknown");
        await using var registry = await OpenAsync(dir, GroupId, cancellationToken);
        var service = new ReplicaFollower(registry);

        _ = await Assert.That(await service.GetStatusAsync("missing", cancellationToken)).IsNull();
    }

    private static FollowerBatch Batch(string leader, ulong term, ulong prevIndex, ulong prevTerm, ulong commit, params ReplicaLogRecord[] records) =>
        new(records, leader, term, prevIndex, prevTerm, commit);

    private static async Task<ReplicaGroupRegistry> OpenAsync(TempDirectory dir, string groupId, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, [groupId], 1, Fingerprint, 1UL);
        await registry.OpenAsync(cancellationToken);
        return registry;
    }

    private static async Task<byte[]> PublishAsync(TempDirectory dir, GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, snapshot.GroupId);
        Directory.CreateDirectory(Path.GetDirectoryName(snapshotPath)!);
        var store = new GroupSnapshotStore(dir, snapshot.GroupId);
        await store.PublishAsync(snapshot, cancellationToken);
        return await File.ReadAllBytesAsync(snapshotPath, cancellationToken);
    }

    private static ReplicaLogRecord Record(ulong index, ulong term) => new(
        index,
        term,
        $"op-{index}",
        "scope",
        new[] { Convert.ToByte(index) },
        "UserMutation",
        "cache",
        new[] { Convert.ToByte(index + 1) },
        "set",
        new[] { Convert.ToByte(index + 2) },
        new[] { Convert.ToByte(index + 3) },
        0,
        0,
        0,
        0);

    private static ReplicaSnapshotUpload Upload(byte[] fileBytes, GroupSnapshot snapshot) => new(
        fileBytes,
        BinaryPrimitives.ReadUInt32LittleEndian(fileBytes.AsSpan(fileBytes.Length - 4)),
        snapshot.LastIncludedIndex,
        snapshot.LastIncludedTerm);
}
