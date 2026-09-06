using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Follower replication over real group logs: append, commit, status, and snapshot install.</summary>
public sealed class ReplicaFollowerTests : NodeIntegrationTestBase
{
    private const string GroupId = "follower-service";
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Commit advances within the durable prefix and refuses beyond it.</summary>
    [Fact]
    public async Task AdvanceCommitAcceptedWithinPrefix()
    {
        using var dir = new TempDirectory("squirix-follower-commit");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), DefaultCancellationToken);

        var advanced = await service.AdvanceCommitAsync(GroupId, Fingerprint, 1UL, 2UL, 1UL, DefaultCancellationToken);

        Assert.True(advanced.Success);
        Assert.Equal(2UL, advanced.CommitIndex);

        var beyond = await service.AdvanceCommitAsync(GroupId, Fingerprint, 1UL, 5UL, 1UL, DefaultCancellationToken);

        Assert.False(beyond.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, beyond.RefusalCode);
    }

    /// <summary>Advancing an unserved group is refused without touching storage.</summary>
    [Fact]
    public async Task AdvanceCommitUnknownGroupRefused()
    {
        using var dir = new TempDirectory("squirix-follower-commit-unknown");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        var result = await service.AdvanceCommitAsync("missing", Fingerprint, 1UL, 1UL, 1UL, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.RefusalCode);
    }

    /// <summary>Accepted appends advance the durable log and are visible in status.</summary>
    [Fact]
    public async Task AppendAcceptedAdvancesLog()
    {
        using var dir = new TempDirectory("squirix-follower-append");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL), Record(2UL, 1UL)), DefaultCancellationToken);

        Assert.True(result.Success);
        Assert.Equal(1UL, result.CurrentTerm);
        Assert.Equal(2UL, result.LastLogIndex);
        var live = await service.GetStatusAsync(GroupId, DefaultCancellationToken);
        var current = Assert.NotNull(live);
        Assert.Equal(2UL, current.LastLogIndex);
    }

    /// <summary>An append from a deposed leader term is refused without touching the log.</summary>
    [Fact]
    public async Task AppendStaleTermRefused()
    {
        using var dir = new TempDirectory("squirix-follower-stale");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);
        _ = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 2UL, 0UL, 0UL, 0UL, Record(1UL, 2UL)), DefaultCancellationToken);

        var result = await service.AppendAsync(GroupId, Fingerprint, 1UL, Batch("leader", 1UL, 1UL, 2UL, 0UL, Record(2UL, 1UL)), DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.StaleTerm, result.RefusalCode);
        Assert.Equal(2UL, result.CurrentTerm);
        var stale = await service.GetStatusAsync(GroupId, DefaultCancellationToken);
        var staleStatus = Assert.NotNull(stale);
        Assert.Equal(1UL, staleStatus.LastLogIndex);
    }

    /// <summary>Appending to an unserved group is refused without touching storage.</summary>
    [Fact]
    public async Task AppendToUnknownGroupRefused()
    {
        using var dir = new TempDirectory("squirix-follower-unknown");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        var result = await service.AppendAsync("missing", Fingerprint, 1UL, Batch("leader", 1UL, 0UL, 0UL, 0UL, Record(1UL, 1UL)), DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.RefusalCode);
    }

    /// <summary>Conflicting fingerprints and older generations are refused as topology mismatches.</summary>
    [Fact]
    public async Task AppendTopologyMismatchRefused()
    {
        using var dir = new TempDirectory("squirix-follower-topology");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        Assert.True((await service.InstallSnapshotAsync(GroupId, Fingerprint, 1UL, snapshot, 1UL, DefaultCancellationToken)).Success);

        var fingerprint = await service.AppendAsync(GroupId, new byte[] { 4 }, 1UL, Batch("leader", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), DefaultCancellationToken);

        Assert.False(fingerprint.Success);
        Assert.Equal(FollowerLogRefusal.TopologyMismatch, fingerprint.RefusalCode);

        var generation = await service.AppendAsync(GroupId, Fingerprint, 0UL, Batch("leader", 1UL, 1UL, 1UL, 1UL, Record(2UL, 1UL)), DefaultCancellationToken);

        Assert.False(generation.Success);
        Assert.Equal(FollowerLogRefusal.TopologyMismatch, generation.RefusalCode);
    }

    /// <summary>A transferred snapshot file installs through validation into the group log.</summary>
    [Fact]
    public async Task InstallSnapshotUploadAccepted()
    {
        using var dir = new TempDirectory("squirix-follower-upload");
        using var sourceDir = new TempDirectory("squirix-follower-upload-source");
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        var fileBytes = await PublishAsync(sourceDir, snapshot);
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        var result = await service.InstallSnapshotUploadAsync(GroupId, Fingerprint, 1UL, Upload(fileBytes, snapshot), 1UL, DefaultCancellationToken);

        Assert.True(result.Success);
        var installed = await service.GetStatusAsync(GroupId, DefaultCancellationToken);
        var installedStatus = Assert.NotNull(installed);
        Assert.Equal(1UL, installedStatus.LastLogIndex);
        Assert.Equal(1UL, installedStatus.CommitIndex);
    }

    /// <summary>A corrupted snapshot transfer is refused without touching storage.</summary>
    [Fact]
    public async Task InstallSnapshotUploadCorruptRefused()
    {
        using var dir = new TempDirectory("squirix-follower-upload-corrupt");
        using var sourceDir = new TempDirectory("squirix-follower-upload-corrupt-source");
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>());
        var fileBytes = await PublishAsync(sourceDir, snapshot);
        fileBytes[^1] ^= 0xFF;
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        var result = await service.InstallSnapshotUploadAsync(GroupId, Fingerprint, 1UL, Upload(fileBytes, snapshot), 1UL, DefaultCancellationToken);

        Assert.False(result.Success);
        var rejected = await service.GetStatusAsync(GroupId, DefaultCancellationToken);
        var rejectedStatus = Assert.NotNull(rejected);
        Assert.Equal(0UL, rejectedStatus.LastLogIndex);
    }

    /// <summary>Codec round-trips every record field and rejects truncation and trailing bytes.</summary>
    [Fact]
    public void LogCodecRoundTripsRecord()
    {
        var record = Record(7UL, 3UL);
        var bytes = ReplicaLogCodec.Encode(in record);

        var decoded = Assert.NotNull(ReplicaLogCodec.Decode(bytes));
        Assert.Equal(bytes, ReplicaLogCodec.Encode(in decoded));
        Assert.Null(ReplicaLogCodec.Decode(bytes.AsMemory(0, bytes.Length - 1)));

        var trailed = new byte[bytes.Length + 1];
        bytes.CopyTo(trailed, 0);
        Assert.Null(ReplicaLogCodec.Decode(trailed));
    }

    /// <summary>Status of an unserved group is null.</summary>
    [Fact]
    public async Task StatusUnknownGroupIsNull()
    {
        using var dir = new TempDirectory("squirix-follower-status-unknown");
        await using var registry = await OpenAsync(dir, GroupId);
        var service = new ReplicaFollower(registry);

        Assert.Null(await service.GetStatusAsync("missing", DefaultCancellationToken));
    }

    private static FollowerBatch Batch(string leader, ulong term, ulong prevIndex, ulong prevTerm, ulong commit, params ReplicaLogRecord[] records) =>
        new(records, leader, term, prevIndex, prevTerm, commit);

    private static async Task<ReplicaGroupRegistry> OpenAsync(TempDirectory dir, string groupId)
    {
        var registry = new ReplicaGroupRegistry(dir.Path, [groupId], 1, Fingerprint, 1UL);
        await registry.OpenAsync(DefaultCancellationToken);
        return registry;
    }

    private static async Task<byte[]> PublishAsync(TempDirectory dir, GroupSnapshot snapshot)
    {
        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir.Path, snapshot.GroupId);
        Directory.CreateDirectory(Path.GetDirectoryName(snapshotPath)!);
        var store = new GroupSnapshotStore(dir.Path, snapshot.GroupId);
        await store.PublishAsync(snapshot, DefaultCancellationToken);
        return await File.ReadAllBytesAsync(snapshotPath, DefaultCancellationToken);
    }

    private static ReplicaLogRecord Record(ulong index, ulong term) => new(
        index,
        term,
        $"op-{index}",
        "scope",
        new byte[] { Convert.ToByte(index) },
        "UserMutation",
        "cache",
        new byte[] { Convert.ToByte(index + 1) },
        "set",
        new byte[] { Convert.ToByte(index + 2) },
        new byte[] { Convert.ToByte(index + 3) },
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
