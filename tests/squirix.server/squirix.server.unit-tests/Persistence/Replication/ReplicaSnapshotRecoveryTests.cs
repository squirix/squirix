using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Recovery and installation behavior of replica-group snapshots.</summary>
public sealed class ReplicaSnapshotRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-snapshot";

    /// <summary>
    /// A successfully compacted durable log survives a crash (dispose) and reopens Ready with the snapshot base,
    /// committed boundaries, and exported idempotency outcomes intact — no mixed-state data loss.
    /// </summary>
    [Fact]
    public async Task CompactedLogIsDurableAndRecoverable()
    {
        using var dir = new TempDirectory("squirix-compact-durable");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            for (var index = 1UL; index <= 8UL; index++)
                _ = await log.AppendAsync(Append(index, 1UL, "durable"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(8UL, DefaultCancellationToken);
            _ = log.Idempotency.Reserve("client", "op-1", new byte[] { 1 }, GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = log.Idempotency.TryResolve("client", "op-1", new byte[] { 9 }, 1UL, 1UL);
            _ = await log.AdvanceAppliedAsync(8UL, DefaultCancellationToken);
            _ = await log.CreateSnapshotAsync(8UL, DefaultCancellationToken);
            var compact = await log.CompactAsync(DefaultCancellationToken);
            Assert.True(compact.Success);
            Assert.NotNull(log.SnapshotPath);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(8UL, status.CommitIndex);
        Assert.Equal(8UL, status.LastLogIndex);
        Assert.Equal(8UL, status.LastAppliedIndex);
        Assert.NotNull(reopened.SnapshotPath);
        Assert.Equal(GroupIdempotencyLookup.Found, reopened.Idempotency.Lookup("client", "op-1", new byte[] { 1 }, out var record));
        Assert.Equal(new byte[] { 9 }, record.OutcomePayload.ToArray());
    }

    /// <summary>
    /// Recovery refuses a published snapshot whose topology fingerprint conflicts with the durable metadata,
    /// failing readiness without restoring outcomes or watermarks from the incompatible snapshot.
    /// </summary>
    [Fact]
    public async Task TopologyConflictFailsReadiness()
    {
        using var sourceDir = new TempDirectory("squirix-conflicting-topology-source");
        using var targetDir = new TempDirectory("squirix-conflicting-topology-target");
        var composition = GroupComposition.Create(GroupId);
        var fingerprint = new byte[] { 1, 2, 3, 4 };

        await using (var seed = new FollowerLog(sourceDir, GroupId, composition))
            await seed.OpenAsync(DefaultCancellationToken);

        await WriteMetadataAsync(sourceDir, new GroupLogMetadata(GroupId, fingerprint, 2UL, 0UL, string.Empty, 0UL, 0UL, 0UL), DefaultCancellationToken);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, 1UL, "snapshot"), DefaultCancellationToken);
            _ = source.Idempotency.Reserve("client", "operation-1", new byte[] { 1, 2, 3 }, GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = source.Idempotency.TryResolve("client", "operation-1", new byte[] { 9 }, 1UL, 1UL);
            _ = await source.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
            await target.OpenAsync(DefaultCancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, new byte[] { 5, 6, 7, 8 }, 2UL, 0UL, string.Empty, 0UL, 0UL, 0UL), DefaultCancellationToken);

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));

        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
        var reopenedStatus = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(new byte[] { 5, 6, 7, 8 }, reopenedStatus.TopologyFingerprint.ToArray());
        Assert.Equal(0UL, reopenedStatus.CommitIndex);
        Assert.Equal(0UL, reopenedStatus.LastAppliedIndex);
        Assert.Equal(GroupIdempotencyLookup.Miss, reopened.Idempotency.Lookup("client", "operation-1", new byte[] { 1, 2, 3 }, out _));
    }

    /// <summary>An outcome resolved exactly at Unix epoch survives the snapshot round-trip and remains resolved.</summary>
    [Fact]
    public async Task EpochOutcomeSurvivesSnapshotTrip()
    {
        using var dir = new TempDirectory("squirix-epoch-snapshot");
        var composition = GroupComposition.Create(GroupId);
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var options = new FollowerLogOptions { TimeProvider = clock, IdempotencyRetention = TimeSpan.FromHours(1) };

        await using (var log = new FollowerLog(dir, GroupId, composition, options))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = log.Idempotency.Reserve("client", "op-epoch", new byte[] { 1 }, GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = log.Idempotency.TryResolve("client", "op-epoch", new byte[] { 9 }, 1UL, 1UL);
            Assert.True(log.Idempotency.Lookup("client", "op-epoch", new byte[] { 1 }, out var preRecord) is GroupIdempotencyLookup.Found);
            Assert.Equal(DateTime.UnixEpoch, preRecord.ResolvedUtc!.Value);
            _ = await log.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, options);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(GroupIdempotencyLookup.Found, reopened.Idempotency.Lookup("client", "op-epoch", new byte[] { 1 }, out var restored));
        Assert.True(restored.IsResolved);
        Assert.Equal(DateTime.UnixEpoch, restored.ResolvedUtc!.Value);
        clock.Advance(TimeSpan.FromHours(2));
        reopened.Idempotency.Expire();
        Assert.Equal(GroupIdempotencyLookup.Miss, reopened.Idempotency.Lookup("client", "op-epoch", new byte[] { 1 }, out _));
    }

    /// <summary>Installing a higher-term snapshot clears a vote from the older term and persists the reset.</summary>
    [Fact]
    public async Task HigherTermSnapshotClearsPreviousVote()
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-vote-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-vote-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(DefaultCancellationToken);
        _ = await source.AppendAsync(Append(1UL, 3UL, "snapshot"), DefaultCancellationToken);
        _ = await source.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        var snapshot = await source.CreateSnapshotAsync(1UL, DefaultCancellationToken);

        await using (var initialTarget = new FollowerLog(targetDir, GroupId, composition))
            await initialTarget.OpenAsync(DefaultCancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, "node-1", 0UL, 0UL, 0UL), DefaultCancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(DefaultCancellationToken);
            Assert.Equal("node-1", (await target.GetStatusAsync(DefaultCancellationToken)).VotedFor);

            var result = await target.InstallSnapshotAsync(snapshot, DefaultCancellationToken);
            var status = await target.GetStatusAsync(DefaultCancellationToken);

            Assert.True(result.Success);
            Assert.Equal(3UL, status.CurrentTerm);
            Assert.Equal(string.Empty, status.VotedFor);
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var persistedStatus = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, persistedStatus.CurrentTerm);
        Assert.Equal(string.Empty, persistedStatus.VotedFor);
    }

    /// <summary>A snapshot must discard a local suffix when its boundary term diverges.</summary>
    [Fact]
    public async Task InstalledSnapshotDiscardsDivergentSuffix()
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-divergent-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-divergent-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(DefaultCancellationToken);
        _ = await source.AppendAsync(Append(1UL, "source-1"), DefaultCancellationToken);
        _ = await source.AppendAsync(
            new FollowerLogAppendRequest("leader", 2UL, 1UL, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("source-2"))])),
            DefaultCancellationToken);
        _ = await source.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        var snapshot = await source.CreateSnapshotAsync(2UL, DefaultCancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(DefaultCancellationToken);
            _ = await target.AppendAsync(Append(1UL, "target-1"), DefaultCancellationToken);
            _ = await target.AppendAsync(Append(2UL, "target-2"), DefaultCancellationToken);
            _ = await target.AppendAsync(Append(3UL, "target-tail"), DefaultCancellationToken);

            var result = await target.InstallSnapshotAsync(snapshot, DefaultCancellationToken);
            Assert.True(result.Success);
            Assert.Equal(2UL, (await target.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
            Assert.Empty(await target.GetUncommittedTailAsync(DefaultCancellationToken));
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, reopenedStatus.LastLogIndex);
        Assert.Empty(await reopened.GetUncommittedTailAsync(DefaultCancellationToken));
    }

    /// <summary>Installing a snapshot restores watermarks, retained idempotency, and a divergent tail.</summary>
    [Fact]
    public async Task InstalledSnapshotRestoresState()
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(DefaultCancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = source.Idempotency.Reserve("client", "operation-1", new byte[] { 1, 2, 3 }, GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = source.Idempotency.TryResolve("client", "operation-1", new byte[] { 9 }, 1UL, 1UL);
        _ = await source.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        var snapshot = await source.CreateSnapshotAsync(2UL, DefaultCancellationToken);

        await using var target = new FollowerLog(targetDir, GroupId, composition);
        await target.OpenAsync(DefaultCancellationToken);
        _ = await target.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await target.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await target.AppendAsync(Append(3UL, "old-tail"), DefaultCancellationToken);
        _ = target.Idempotency.Reserve("client", "pending-tail", new byte[] { 4 }, GroupRecordKind.UserMutation, 3UL, 1UL);

        var result = await target.InstallSnapshotAsync(snapshot, DefaultCancellationToken);
        var status = await target.GetStatusAsync(DefaultCancellationToken);

        Assert.True(result.Success);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(0UL, status.LastAppliedIndex);
        Assert.Equal(3UL, status.LastLogIndex);
        Assert.Equal("old-tail", Encoding.UTF8.GetString((await target.GetUncommittedTailAsync(DefaultCancellationToken))[0].Payload.ToArray()));
        Assert.Equal(GroupId, snapshot.GroupId);
        Assert.NotNull(target.SnapshotPath);
        Assert.Equal(GroupIdempotencyLookup.Found, target.Idempotency.Lookup("client", "operation-1", new byte[] { 1, 2, 3 }, out var record));
        Assert.Equal(new byte[] { 9 }, record.OutcomePayload.ToArray());
        Assert.Equal(GroupIdempotencyLookup.Unresolved, target.Idempotency.Lookup("client", "pending-tail", new byte[] { 4 }, out var pending));
        Assert.True(pending.IsUnresolved);
    }

    /// <summary>
    /// An installed snapshot with committed idempotency outcomes survives a crash (dispose) and reopen,
    /// restoring watermarks, the retained tail, and the resolved outcome.
    /// </summary>
    [Fact]
    public async Task InstalledSnapshotSurvivesCrashAndReopen()
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-crash-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-crash-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(DefaultCancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = source.Idempotency.Reserve("client", "operation-1", new byte[] { 1, 2, 3 }, GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = source.Idempotency.TryResolve("client", "operation-1", new byte[] { 9 }, 1UL, 1UL);
        _ = await source.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        var snapshot = await source.CreateSnapshotAsync(2UL, DefaultCancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(DefaultCancellationToken);
            var result = await target.InstallSnapshotAsync(snapshot, DefaultCancellationToken);
            Assert.True(result.Success);
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(2UL, status.LastAppliedIndex);
        Assert.Equal(2UL, status.LastLogIndex);
        Assert.Equal(GroupIdempotencyLookup.Found, reopened.Idempotency.Lookup("client", "operation-1", new byte[] { 1, 2, 3 }, out var record));
        Assert.Equal(new byte[] { 9 }, record.OutcomePayload.ToArray());
    }

    /// <summary>An oversized snapshot file is rejected before allocating memory for its contents.</summary>
    [Fact]
    public async Task OversizedSnapshotRejectedOnOpen()
    {
        using var dir = new TempDirectory("squirix-oversized-snapshot");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        await File.WriteAllBytesAsync(snapshotPath, new byte[128], DefaultCancellationToken);

        var options = new FollowerLogOptions { MaxSnapshotBytes = 64 };
        await using var reopened = new FollowerLog(dir, GroupId, composition, options);
        var exception = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("exceeds the maximum configured size of 64 bytes", exception.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>Publish refuses a snapshot whose invariants the on-disk decoder would later reject.</summary>
    [Fact]
    public async Task PublishRejectsCommitBelowIncluded()
    {
        using var dir = new TempDirectory("squirix-snapshot-publish-reject");
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
            await seed.OpenAsync(DefaultCancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        var store = new GroupSnapshotStore(dir, GroupId);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(malformed, DefaultCancellationToken));

        Assert.False(store.SnapshotExists);
    }

    /// <summary>
    /// Publication refuses a zero included term in line with the recovery guard, and a hand-crafted zero-term
    /// snapshot file still fails recovery readiness instead of installing an unverifiable baseline.
    /// </summary>
    [Fact]
    public async Task RecoveryRejectsZeroIncludedTerm()
    {
        using var dir = new TempDirectory("squirix-recovery-zero-term");
        var composition = GroupComposition.Create(GroupId);

        // Publication mirrors the recovery refusal: a non-empty snapshot with an unverifiable zero included term
        // must never replace a previously published, readable snapshot.
        var store = new GroupSnapshotStore(dir, GroupId);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            store.PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>()), DefaultCancellationToken));

        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(DefaultCancellationToken);
            _ = await seed.AppendAsync(Append(1UL, 1UL, "snapshot"), DefaultCancellationToken);
            _ = await seed.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await seed.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        // The corrupt state can no longer be produced through PublishAsync, so patch the published file directly,
        // exactly as an externally corrupted file would appear, and restore the CRC.
        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        var bytes = await File.ReadAllBytesAsync(snapshotPath, DefaultCancellationToken);
        var payloadLength = SnapshotTestLayout.ReadPayloadLength(bytes);

        // Guard the assumed field position: the seed snapshot carries included term 1, so a layout change is
        // detected before the patch lands on an unrelated field.
        var termOnDisk = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount + SnapshotTestLayout.LastIncludedTermPayloadOffset, 8));
        Assert.Equal(1UL, termOnDisk);

        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount + SnapshotTestLayout.LastIncludedTermPayloadOffset, 8), 0UL);
        var compute = Crc32C.Compute(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount, payloadLength));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(SnapshotTestLayout.CrcFileOffset(payloadLength), 4), compute);
        await File.WriteAllBytesAsync(snapshotPath, bytes, DefaultCancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        var exception = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("included term is zero", exception.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>
    /// A crash after snapshot publication but before journal truncation must discard the divergent journal
    /// suffix instead of restoring it for replication.
    /// </summary>
    [Fact]
    public async Task RecoveryDiscardsDivergentSuffix()
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-divergent-crash-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-divergent-crash-target");
        var composition = GroupComposition.Create(GroupId);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, "source-1"), DefaultCancellationToken);
            _ = await source.AppendAsync(
                new FollowerLogAppendRequest(
                    "leader",
                    2UL,
                    1UL,
                    1UL,
                    0UL,
                    new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("source-2"))])),
                DefaultCancellationToken);
            _ = await source.AdvanceCommitAsync(2UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(2UL, DefaultCancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(DefaultCancellationToken);
            _ = await target.AppendAsync(Append(1UL, "target-1"), DefaultCancellationToken);
            _ = await target.AppendAsync(Append(2UL, "target-2"), DefaultCancellationToken);
            _ = await target.AppendAsync(Append(3UL, "target-tail"), DefaultCancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, string.Empty, 3UL, 2UL, 2UL), DefaultCancellationToken);

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(2UL, status.LastLogIndex);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(2UL, status.LastAppliedIndex);
        Assert.Empty(await reopened.GetUncommittedTailAsync(DefaultCancellationToken));

        var memory = new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("resumed-3"))]);
        var request = new FollowerLogAppendRequest("leader", 2UL, 2UL, 2UL, 0UL, memory);
        var resumed = await reopened.AppendAsync(request, DefaultCancellationToken);
        Assert.True(resumed.Success);
        Assert.Equal(3UL, (await reopened.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        var single = Assert.Single(tail);
        Assert.Equal("resumed-3", Encoding.UTF8.GetString(single.Payload.ToArray()));
    }

    /// <summary>
    /// Recovery reconciles a higher-term published snapshot's term, vote, topology fingerprint, and
    /// configuration generation with stale durable metadata after a crash between snapshot publication and
    /// metadata persistence.
    /// </summary>
    [Fact]
    public async Task RecoveryAdoptsHigherSnapshotTerm()
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-higher-term-source");
        using var targetDir = new TempDirectory("squirix-snapshot-higher-term-target");
        var composition = GroupComposition.Create(GroupId);
        var fingerprint = new byte[] { 1, 2, 3, 4 };

        await using (var seed = new FollowerLog(sourceDir, GroupId, composition))
            await seed.OpenAsync(DefaultCancellationToken);

        await WriteMetadataAsync(sourceDir, new GroupLogMetadata(GroupId, fingerprint, 5UL, 0UL, string.Empty, 0UL, 0UL, 0UL), DefaultCancellationToken);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, 3UL, "snapshot"), DefaultCancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
            await target.OpenAsync(DefaultCancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, "node-1", 0UL, 0UL, 0UL), DefaultCancellationToken);

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, status.CurrentTerm);
        Assert.Equal(string.Empty, status.VotedFor);
        Assert.Equal(fingerprint, status.TopologyFingerprint.ToArray());
        Assert.Equal(5UL, status.ConfigurationGeneration);
        Assert.Equal(1UL, status.CommitIndex);
        Assert.Equal(1UL, status.LastAppliedIndex);
        Assert.Equal(1UL, status.LastLogIndex);
    }

    /// <summary>
    /// Recovery reconciles a newer snapshot commit and applied watermarks with stale durable metadata
    /// that trails the published snapshot after a crash.
    /// </summary>
    [Fact]
    public async Task RecoveryReconcilesSnapshotWatermarks()
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-watermarks-source");
        using var targetDir = new TempDirectory("squirix-snapshot-watermarks-target");
        var composition = GroupComposition.Create(GroupId);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await source.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
            _ = await source.AdvanceCommitAsync(2UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(2UL, DefaultCancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(DefaultCancellationToken);

            // The payloads differ from the source on purpose: reconciliation compares the boundary term, not the
            // payloads below the boundary, so the commit watermark still advances to the snapshot boundary.
            //
            // This divergence is constructed intentionally to violate the log matching property, under which
            // entries at the same index and term have identical content, so equal terms imply equal payloads
            // below the snapshot boundary. Proving recovery decides by term alone rules out that it merely
            // accepts genuinely divergent committed content.
            _ = await target.AppendAsync(Append(1UL, "x"), DefaultCancellationToken);
            _ = await target.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await target.AppendAsync(Append(2UL, "y"), DefaultCancellationToken);
            _ = await target.AppendAsync(Append(3UL, "z"), DefaultCancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(0UL, status.LastAppliedIndex);
        Assert.Equal(3UL, status.LastLogIndex);
        Assert.NotNull(reopened.SnapshotPath);
    }

    /// <summary>Snapshot encoding rejects a null committed-outcomes list with the documented argument exception.</summary>
    [Fact]
    public async Task SnapshotEncodingRejectsNullOutcomes()
    {
        using var dir = new TempDirectory("squirix-snapshot-null-outcomes");
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
            await seed.OpenAsync(DefaultCancellationToken);

        var nullOutcomes = default(GroupSnapshot) with { GroupId = GroupId };
        var store = new GroupSnapshotStore(dir, GroupId);
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(store.PublishAsync(nullOutcomes, DefaultCancellationToken));
    }

    /// <summary>Snapshot-only startup validates the published snapshot instead of treating the directory as fresh state.</summary>
    [Fact]
    public async Task StartupRejectsForeignGroupSnapshot()
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-only-source");
        using var targetDir = new TempDirectory("squirix-snapshot-only-target");
        const string sourceGroupId = "grp-snapshot-source";
        var targetComposition = GroupComposition.Create(GroupId);

        Directory.CreateDirectory(targetDir.Path);

        await using (var source = new FollowerLog(sourceDir, sourceGroupId, GroupComposition.Create(sourceGroupId)))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, 1UL, "snapshot"), DefaultCancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        Directory.CreateDirectory(Path.GetDirectoryName(GroupStoragePaths.GetSnapshotPath(targetDir, GroupId))!);
        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, sourceGroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, targetComposition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));

        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>Recovery refuses a published snapshot whose commit index falls below its included index, so it never persists an applied watermark above the commit watermark.</summary>
    [Fact]
    public async Task CommitBelowIncludedSnapshotFails()
    {
        using var dir = new TempDirectory("squirix-snapshot-commit-below-included");
        var composition = GroupComposition.Create(GroupId);

        // Publish a valid snapshot, then patch its on-disk commit index below the included index and restore the
        // CRC, exactly as an externally corrupted file would appear. PublishAsync itself now validates these
        // invariants, so the corrupt state can only be produced directly on disk.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
            await seed.OpenAsync(DefaultCancellationToken);
        await new GroupSnapshotStore(dir, GroupId).PublishAsync(
            new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 7UL, Array.Empty<GroupIdempotencyRecord>()),
            DefaultCancellationToken);

        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        var bytes = await File.ReadAllBytesAsync(snapshotPath, DefaultCancellationToken);
        var payloadLength = SnapshotTestLayout.ReadPayloadLength(bytes);

        // Guard the assumed field position: only the commit index holds 7, so a layout change is detected.
        var commitIndexOnDisk = SnapshotTestLayout.ReadCommitIndex(bytes);
        Assert.True(
            commitIndexOnDisk == 7UL,
            $"Snapshot layout changed: expected the commit index 7 at payload offset {SnapshotTestLayout.CommitIndexPayloadOffset}, read {commitIndexOnDisk}. Update the offsets in SnapshotTestLayout.");
        SnapshotTestLayout.WriteCommitIndex(bytes, 2UL);
        BinaryPrimitives.WriteUInt32LittleEndian(
            bytes.AsSpan(SnapshotTestLayout.CrcFileOffset(payloadLength), 4),
            Crc32C.Compute(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount, payloadLength)));
        await File.WriteAllBytesAsync(snapshotPath, bytes, DefaultCancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(log.OpenAsync(DefaultCancellationToken));

        Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);
    }

    /// <summary>A snapshot containing an unresolved outcome is rejected before publication.</summary>
    [Fact]
    public async Task SnapshotWithUnresolvedOutcomeIsRejected()
    {
        using var dir = new TempDirectory("squirix-unresolved-snapshot");
        var composition = GroupComposition.Create(GroupId);

        await using (var seed = new FollowerLog(dir, GroupId, composition))
            await seed.OpenAsync(DefaultCancellationToken);

        var store = new GroupSnapshotStore(dir, GroupId);
        var unresolvedRecord = new GroupIdempotencyRecord(
            "client",
            "unresolved-op",
            new byte[] { 1 },
            ReadOnlyMemory<byte>.Empty,
            GroupRecordKind.UserMutation,
            DateTime.UnixEpoch,
            null,
            1UL,
            1UL);
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 1UL, 1UL, 1UL, 1UL, [unresolvedRecord]);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidOperationException>(store.PublishAsync(snapshot, DefaultCancellationToken));
        Assert.False(store.SnapshotExists);
    }

    /// <summary>The store rejects a published snapshot whose embedded group id differs from its own.</summary>
    [Fact]
    public async Task StoreRejectsSnapshotFromAnotherGroup()
    {
        using var dir = new TempDirectory("squirix-store-wrong-group");

        // The log seed creates the on-disk replication layout that PublishAsync writes into.
        await using (var seed = new FollowerLog(dir, "grp-a", GroupComposition.Create("grp-a")))
            await seed.OpenAsync(DefaultCancellationToken);

        var storeA = new GroupSnapshotStore(dir, "grp-a");
        await storeA.PublishAsync(new GroupSnapshot("grp-a", ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>()), DefaultCancellationToken);

        var sourcePath = GroupStoragePaths.GetSnapshotPath(dir, "grp-a");
        var targetPath = GroupStoragePaths.GetSnapshotPath(dir, "grp-b");
        Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);
        File.Copy(sourcePath, targetPath);

        var storeB = new GroupSnapshotStore(dir, "grp-b");
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(storeB.ReadPublishedAsync(DefaultCancellationToken));

        Assert.Contains("different group", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>A torn first suffix frame after a compacted prefix can be truncated rather than crashing recovery.</summary>
    [Fact]
    public async Task TornFirstSuffixFrameCanBeTruncated()
    {
        using var dir = new TempDirectory("squirix-torn-suffix");
        var composition = GroupComposition.Create(GroupId);
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(4UL, "d"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(5UL, "e"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(3UL, DefaultCancellationToken);
            _ = await log.AdvanceAppliedAsync(3UL, DefaultCancellationToken);
            _ = await log.CreateSnapshotAsync(3UL, DefaultCancellationToken);
            var result = await log.CompactAsync(DefaultCancellationToken);
            Assert.True(result.Success);
        }

        var bytes = await File.ReadAllBytesAsync(logPath, DefaultCancellationToken);

        // Skip the file header and the first frame's preamble; the four overwritten bytes are the frame's
        // body-length field, so the frame is read as torn rather than as a corrupted body.
        var corruptionOffset = GroupLogCodec.LogFileHeader.Length + GroupLogCodec.FramePreambleByteCount;
        Assert.True(bytes.Length > corruptionOffset + sizeof(int));
        bytes[corruptionOffset] = 0x88;
        bytes[corruptionOffset + 1] = 0x13;
        bytes[corruptionOffset + 2] = 0x00;
        bytes[corruptionOffset + 3] = 0x00;
        await File.WriteAllBytesAsync(logPath, bytes, DefaultCancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, status.CommitIndex);
        Assert.Equal(3UL, status.LastAppliedIndex);
        Assert.Equal(3UL, status.LastLogIndex);
    }

    /// <summary>
    /// A torn first suffix frame above a commit watermark that exceeds the snapshot base fails readiness
    /// without truncating the journal, so the committed frames stay on disk for repair.
    /// </summary>
    [Fact]
    public async Task TornSuffixAboveCommitMarkFails()
    {
        using var dir = new TempDirectory("squirix-torn-suffix-committed");
        var composition = GroupComposition.Create(GroupId);
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(3UL, DefaultCancellationToken);
            _ = await log.AdvanceAppliedAsync(3UL, DefaultCancellationToken);
            _ = await log.CreateSnapshotAsync(3UL, DefaultCancellationToken);
            var result = await log.CompactAsync(DefaultCancellationToken);
            Assert.True(result.Success);

            // Append and commit past the boundary so the durable commit watermark exceeds the
            // snapshot base while the suffix frames stay outside the snapshot's coverage.
            _ = await log.AppendAsync(Append(4UL, "d"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(5UL, "e"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(5UL, DefaultCancellationToken);
        }

        // The durable commit watermark (five) now sits above the compaction boundary (three).
        Assert.Equal(5UL, await ReadDurableCommitIndexAsync(dir));

        var bytes = await File.ReadAllBytesAsync(logPath, DefaultCancellationToken);

        // Same torn-shape corruption as the truncatable case: overwrite the first suffix frame's
        // body-length field so the frame reads as torn rather than as a corrupted body.
        var corruptionOffset = GroupLogCodec.LogFileHeader.Length + GroupLogCodec.FramePreambleByteCount;
        Assert.True(bytes.Length > corruptionOffset + sizeof(int));
        bytes[corruptionOffset] = 0x88;
        bytes[corruptionOffset + 1] = 0x13;
        bytes[corruptionOffset + 2] = 0x00;
        bytes[corruptionOffset + 3] = 0x00;
        await File.WriteAllBytesAsync(logPath, bytes, DefaultCancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));

        // Recovery must refuse destructively rewriting the journal: the bytes beyond the header
        // remain on disk so the committed frames above the snapshot base can still be repaired.
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
        Assert.Equal(5UL, await ReadDurableCommitIndexAsync(dir));
        var survivingBytes = await File.ReadAllBytesAsync(logPath, DefaultCancellationToken);
        Assert.True(survivingBytes.Length > GroupLogCodec.LogFileHeader.Length);
    }

    /// <summary>
    /// Recovery after a crash between snapshot publication and the installation log rewrite treats the walked,
    /// shorter journal as covered by the published snapshot instead of failing readiness on every start.
    /// </summary>
    [Fact]
    public async Task SnapshotCoversShortJournalPostCrash()
    {
        using var dir = new TempDirectory("squirix-install-crash-recovery");
        var composition = GroupComposition.Create(GroupId);
        var store = new GroupSnapshotStore(dir, GroupId);

        // Simulate the crash window: the snapshot at index three and the advanced install-candidate metadata
        // are durable, while the log rewrite never ran and the file still ends at index one.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(DefaultCancellationToken);
            _ = await seed.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await seed.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        await store.PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 3UL, 3UL, Array.Empty<GroupIdempotencyRecord>()), DefaultCancellationToken);

        var candidate = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, string.Empty, 3UL, 3UL, 3UL);
        await WriteMetadataAsync(dir, candidate, DefaultCancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);

        // The watermarks describe the installed snapshot; the journal honestly ends below them until
        // replication rebuilds the covered span from the leader.
        Assert.Equal(3UL, status.CommitIndex);
        Assert.Equal(3UL, status.LastAppliedIndex);
        Assert.Equal(1UL, status.LastLogIndex);
    }

    /// <summary>A snapshot belonging to a different group fails readiness on recovery.</summary>
    [Fact]
    public async Task WrongGroupSnapshotFailsReadiness()
    {
        using var sourceDir = new TempDirectory("squirix-wrong-group-source");
        using var targetDir = new TempDirectory("squirix-wrong-group-target");
        var composition = GroupComposition.Create("grp-a");

        await using (var source = new FollowerLog(sourceDir, "grp-a", composition))
        {
            await source.OpenAsync(DefaultCancellationToken);
            _ = await source.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, DefaultCancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, "grp-b", GroupComposition.Create("grp-b")))
        {
            await target.OpenAsync(DefaultCancellationToken);
            _ = await target.AppendAsync(Append(1UL, "b"), DefaultCancellationToken);
            _ = await target.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, "grp-a"), GroupStoragePaths.GetSnapshotPath(targetDir, "grp-b"));

        await using var reopened = new FollowerLog(targetDir, "grp-b", GroupComposition.Create("grp-b"));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>Encodes metadata through the production codec and writes it as the group's durable metadata file.</summary>
    /// <param name="dir">The group directory receiving the metadata file.</param>
    /// <param name="metadata">The metadata instance to encode.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the file has been written.</returns>
    private static Task WriteMetadataAsync(TempDirectory dir, GroupLogMetadata metadata, CancellationToken cancellationToken)
    {
        var encoded = new byte[GroupLogCodec.ComputeMetaEncodedLength(metadata)];
        GroupLogCodec.EncodeMeta(metadata, encoded);
        return File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, metadata.GroupId), encoded, cancellationToken);
    }

    /// <summary>Reads the durable metadata's commit index directly from the encoded metadata file.</summary>
    /// <param name="dir">The temporary directory holding the replication layout.</param>
    /// <returns>The durable commit index.</returns>
    /// <exception cref="InvalidDataException">Thrown when the encoded metadata fails codec validation.</exception>
    private static async Task<ulong> ReadDurableCommitIndexAsync(TempDirectory dir)
    {
        var encoded = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), DefaultCancellationToken);
        return GroupLogCodec.TryDecodeMeta(encoded, out var meta) ? meta.CommitIndex : throw new InvalidDataException("The durable metadata payload failed codec validation.");
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);
}
