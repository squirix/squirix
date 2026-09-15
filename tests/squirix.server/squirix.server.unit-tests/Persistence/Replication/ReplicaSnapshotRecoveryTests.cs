using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Recovery and installation behavior of replica-group snapshots.</summary>
public sealed class ReplicaSnapshotRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-snapshot";

    /// <summary>Recovery refuses a published snapshot whose commit index falls below its included index, so it never persists an applied watermark above the commit watermark.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitBelowIncludedSnapshotFails(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snapshot-commit-below-included");
        var composition = GroupComposition.Create(GroupId);

        // Publish a valid snapshot, then patch its on-disk commit index below the included index and restore the
        // CRC, exactly as an externally corrupted file would appear. PublishAsync itself now validates these
        // invariants, so the corrupt state can only be produced directly on disk.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
            await seed.OpenAsync(cancellationToken);
        await new GroupSnapshotStore(dir, GroupId).PublishAsync(
            new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 7UL, Array.Empty<GroupIdempotencyRecord>()),
            cancellationToken);

        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        var bytes = await File.ReadAllBytesAsync(snapshotPath, cancellationToken);
        var payloadLength = SnapshotTestLayout.ReadPayloadLength(bytes);

        // Guard the assumed field position: only the commit index holds 7, so a layout change is detected.
        var commitIndexOnDisk = SnapshotTestLayout.ReadCommitIndex(bytes);
        _ = await Assert.That(commitIndexOnDisk == 7UL).IsTrue().Because(
            $"Snapshot layout changed: expected the commit index 7 at payload offset {SnapshotTestLayout.CommitIndexPayloadOffset}, read {commitIndexOnDisk}. Update the offsets in SnapshotTestLayout.");
        SnapshotTestLayout.WriteCommitIndex(bytes, 2UL);
        BinaryPrimitives.WriteUInt32LittleEndian(
            bytes.AsSpan(SnapshotTestLayout.CrcFileOffset(payloadLength), 4),
            Crc32C.Compute(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount, payloadLength)));
        await File.WriteAllBytesAsync(snapshotPath, bytes, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>
    /// A successfully compacted durable log survives a crash (dispose) and reopens Ready with the snapshot base,
    /// committed boundaries, and exported idempotency outcomes intact — no mixed-state data loss.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedLogIsDurableAndRecoverable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-durable");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            for (var index = 1UL; index <= 8UL; index++)
                _ = await log.AppendAsync(Append(index, 1UL, "durable"), cancellationToken);
            _ = await log.AdvanceCommitAsync(8UL, cancellationToken);
            _ = log.Idempotency.Reserve("client", "op-1", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = log.Idempotency.TryResolve("client", "op-1", [9], 1UL, 1UL);
            _ = await log.AdvanceAppliedAsync(8UL, cancellationToken);
            _ = await log.CreateSnapshotAsync(8UL, cancellationToken);
            var compact = await log.CompactAsync(cancellationToken);
            _ = await Assert.That(compact.Success).IsTrue();
            _ = await Assert.That(log.SnapshotPath).IsNotNull();
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(8UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(8UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(8UL);
        _ = await Assert.That(reopened.SnapshotPath).IsNotNull();
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "op-1", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.OutcomePayload.Span is [9]).IsTrue();
    }

    /// <summary>An outcome resolved exactly at Unix epoch survives the snapshot round-trip and remains resolved.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EpochOutcomeSurvivesSnapshotTrip(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-epoch-snapshot");
        var composition = GroupComposition.Create(GroupId);
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var options = new FollowerLogOptions { TimeProvider = clock, IdempotencyRetention = TimeSpan.FromHours(1) };

        await using (var log = new FollowerLog(dir, GroupId, composition, options))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            _ = log.Idempotency.Reserve("client", "op-epoch", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = log.Idempotency.TryResolve("client", "op-epoch", [9], 1UL, 1UL);
            _ = await Assert.That(log.Idempotency.Lookup("client", "op-epoch", [1], out var preRecord) is GroupIdempotencyLookup.Found).IsTrue();
            _ = await Assert.That(preRecord.ResolvedUtc!.Value).IsEqualTo(DateTime.UnixEpoch);
            _ = await log.CreateSnapshotAsync(1UL, cancellationToken);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, options);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Idempotency.Lookup("client", "op-epoch", [1], out var restored)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(restored.IsResolved).IsTrue();
        _ = await Assert.That(restored.ResolvedUtc!.Value).IsEqualTo(DateTime.UnixEpoch);
        clock.Advance(TimeSpan.FromHours(2));
        reopened.Idempotency.Expire();
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "op-epoch", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>Installing a higher-term snapshot clears a vote from the older term and persists the reset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermSnapshotClearsPreviousVote(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-vote-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-vote-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, 3UL, "snapshot"), cancellationToken);
        _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
        var snapshot = await source.CreateSnapshotAsync(1UL, cancellationToken);

        await using (var initialTarget = new FollowerLog(targetDir, GroupId, composition))
            await initialTarget.OpenAsync(cancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, "node-1", 0UL, 0UL, 0UL), cancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(cancellationToken);
            _ = await Assert.That((await target.GetStatusAsync(cancellationToken)).VotedFor).IsEqualTo("node-1");

            var result = await target.InstallSnapshotAsync(snapshot, 3UL, cancellationToken);
            var status = await target.GetStatusAsync(cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That(status.CurrentTerm).IsEqualTo(3UL);
            _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);
        var persistedStatus = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(persistedStatus.CurrentTerm).IsEqualTo(3UL);
        _ = await Assert.That(persistedStatus.VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>A snapshot must discard a local suffix when its boundary term diverges.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstalledSnapshotDiscardsDivergentSuffix(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-divergent-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-divergent-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "source-1"), cancellationToken);
        _ = await source.AppendAsync(
            new FollowerLogAppendRequest("leader", 2UL, 1UL, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("source-2"))])),
            cancellationToken);
        _ = await source.AdvanceCommitAsync(2UL, cancellationToken);
        var snapshot = await source.CreateSnapshotAsync(2UL, cancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(cancellationToken);
            _ = await target.AppendAsync(Append(1UL, "target-1"), cancellationToken);
            _ = await target.AppendAsync(Append(2UL, "target-2"), cancellationToken);
            _ = await target.AppendAsync(Append(3UL, "target-tail"), cancellationToken);

            var result = await target.InstallSnapshotAsync(snapshot, 2UL, cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That((await target.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
            _ = await Assert.That(await target.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(reopenedStatus.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(await reopened.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
    }

    /// <summary>Installing a snapshot restores watermarks, retained idempotency, and a divergent tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstalledSnapshotRestoresState(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = source.Idempotency.Reserve("client", "operation-1", [1, 2, 3], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = source.Idempotency.TryResolve("client", "operation-1", [9], 1UL, 1UL);
        _ = await source.AdvanceCommitAsync(2UL, cancellationToken);

        var snapshot = await source.CreateSnapshotAsync(2UL, cancellationToken);

        await using var target = new FollowerLog(targetDir, GroupId, composition);
        await target.OpenAsync(cancellationToken);
        _ = await target.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await target.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await target.AppendAsync(Append(3UL, "old-tail"), cancellationToken);
        _ = target.Idempotency.Reserve("client", "pending-tail", [4], GroupRecordKind.UserMutation, 3UL, 1UL);

        var result = await target.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);
        var status = await target.GetStatusAsync(cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(Encoding.UTF8.GetString((await target.GetUncommittedTailAsync(cancellationToken))[0].Payload.ToArray())).IsEqualTo("old-tail");
        _ = await Assert.That(snapshot.GroupId).IsEqualTo(GroupId);
        _ = await Assert.That(target.SnapshotPath).IsNotNull();
        _ = await Assert.That(target.Idempotency.Lookup("client", "operation-1", [1, 2, 3], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.OutcomePayload.Span is [9]).IsTrue();
        _ = await Assert.That(target.Idempotency.Lookup("client", "pending-tail", [4], out var pending)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(pending.IsUnresolved).IsTrue();
    }

    /// <summary>
    /// An installed snapshot with committed idempotency outcomes survives a crash (dispose) and reopen,
    /// restoring watermarks, the retained tail, and the resolved outcome.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstalledSnapshotSurvivesCrashAndReopen(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-crash-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-crash-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = source.Idempotency.Reserve("client", "operation-1", [1, 2, 3], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = source.Idempotency.TryResolve("client", "operation-1", [9], 1UL, 1UL);
        _ = await source.AdvanceCommitAsync(2UL, cancellationToken);
        var snapshot = await source.CreateSnapshotAsync(2UL, cancellationToken);

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(cancellationToken);
            var result = await target.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "operation-1", [1, 2, 3], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.OutcomePayload.Span is [9]).IsTrue();
    }

    /// <summary>An oversized snapshot file is rejected before allocating memory for its contents.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedSnapshotRejectedOnOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-oversized-snapshot");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        await File.WriteAllBytesAsync(snapshotPath, new byte[128], cancellationToken);

        var options = new FollowerLogOptions { MaxSnapshotBytes = 64 };
        await using var reopened = new FollowerLog(dir, GroupId, composition, options);
        var exception = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(exception.Message).Contains("exceeds the maximum configured size of 64 bytes", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>Publish refuses a snapshot whose invariants the on-disk decoder would later reject.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishRejectsCommitBelowIncluded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snapshot-publish-reject");
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
            await seed.OpenAsync(cancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        var store = new GroupSnapshotStore(dir, GroupId);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(malformed, cancellationToken));

        _ = await Assert.That(store.SnapshotExists).IsFalse();
    }

    /// <summary>
    /// Recovery reconciles a higher-term published snapshot's term, vote, topology fingerprint, and
    /// configuration generation with stale durable metadata after a crash between snapshot publication and
    /// metadata persistence.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryAdoptsHigherSnapshotTerm(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-higher-term-source");
        using var targetDir = new TempDirectory("squirix-snapshot-higher-term-target");
        var composition = GroupComposition.Create(GroupId);
        var fingerprint = new byte[] { 1, 2, 3, 4 };

        await using (var seed = new FollowerLog(sourceDir, GroupId, composition))
            await seed.OpenAsync(cancellationToken);

        await WriteMetadataAsync(sourceDir, new GroupLogMetadata(GroupId, fingerprint, 5UL, 0UL, string.Empty, 0UL, 0UL, 0UL), cancellationToken);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, 3UL, "snapshot"), cancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, cancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
            await target.OpenAsync(cancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, "node-1", 0UL, 0UL, 0UL), cancellationToken);

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(3UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
        await SequenceAssert.Equal(fingerprint, status.TopologyFingerprint.ToArray());
        _ = await Assert.That(status.ConfigurationGeneration).IsEqualTo(5UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(1UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// A crash after snapshot publication but before journal truncation must discard the divergent journal
    /// suffix instead of restoring it for replication.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryDiscardsDivergentSuffix(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-replica-snapshot-divergent-crash-source");
        using var targetDir = new TempDirectory("squirix-replica-snapshot-divergent-crash-target");
        var composition = GroupComposition.Create(GroupId);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, "source-1"), cancellationToken);
            _ = await source.AppendAsync(
                new FollowerLogAppendRequest(
                    "leader",
                    2UL,
                    1UL,
                    1UL,
                    0UL,
                    new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("source-2"))])),
                cancellationToken);
            _ = await source.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(2UL, cancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(cancellationToken);
            _ = await target.AppendAsync(Append(1UL, "target-1"), cancellationToken);
            _ = await target.AppendAsync(Append(2UL, "target-2"), cancellationToken);
            _ = await target.AppendAsync(Append(3UL, "target-tail"), cancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, string.Empty, 3UL, 2UL, 2UL), cancellationToken);

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That(await reopened.GetUncommittedTailAsync(cancellationToken)).IsEmpty();

        var memory = new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("resumed-3"))]);
        var request = new FollowerLogAppendRequest("leader", 2UL, 2UL, 2UL, 0UL, memory);
        var resumed = await reopened.AppendAsync(request, cancellationToken);
        _ = await Assert.That(resumed.Success).IsTrue();
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        var single = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(Encoding.UTF8.GetString(single.Payload.ToArray())).IsEqualTo("resumed-3");
    }

    /// <summary>
    /// Recovery reconciles a newer snapshot commit and applied watermarks with stale durable metadata
    /// that trails the published snapshot after a crash.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryReconcilesSnapshotWatermarks(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-watermarks-source");
        using var targetDir = new TempDirectory("squirix-snapshot-watermarks-target");
        var composition = GroupComposition.Create(GroupId);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await source.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(2UL, cancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
        {
            await target.OpenAsync(cancellationToken);

            // The payloads differ from the source on purpose: reconciliation compares the boundary term, not the
            // payloads below the boundary, so the commit watermark still advances to the snapshot boundary.
            //
            // This divergence is constructed intentionally to violate the log matching property, under which
            // entries at the same index and term have identical content, so equal terms imply equal payloads
            // below the snapshot boundary. Proving recovery decides by term alone rules out that it merely
            // accepts genuinely divergent committed content.
            _ = await target.AppendAsync(Append(1UL, "x"), cancellationToken);
            _ = await target.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await target.AppendAsync(Append(2UL, "y"), cancellationToken);
            _ = await target.AppendAsync(Append(3UL, "z"), cancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(reopened.SnapshotPath).IsNotNull();
    }

    /// <summary>
    /// Publication refuses a zero included term in line with the recovery guard, and a hand-crafted zero-term
    /// snapshot file still fails recovery readiness instead of installing an unverifiable baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryRejectsZeroIncludedTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-recovery-zero-term");
        var composition = GroupComposition.Create(GroupId);

        // Publication mirrors the recovery refusal: a non-empty snapshot with an unverifiable zero included term
        // must never replace a previously published, readable snapshot.
        var store = new GroupSnapshotStore(dir, GroupId);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            store.PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>()), cancellationToken));

        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(cancellationToken);
            _ = await seed.AppendAsync(Append(1UL, 1UL, "snapshot"), cancellationToken);
            _ = await seed.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await seed.CreateSnapshotAsync(1UL, cancellationToken);
        }

        // The corrupt state can no longer be produced through PublishAsync, so patch the published file directly,
        // exactly as an externally corrupted file would appear, and restore the CRC.
        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        var bytes = await File.ReadAllBytesAsync(snapshotPath, cancellationToken);
        var payloadLength = SnapshotTestLayout.ReadPayloadLength(bytes);

        // Guard the assumed field position: the seed snapshot carries included term 1, so a layout change is
        // detected before the patch lands on an unrelated field.
        var termOnDisk = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount + SnapshotTestLayout.LastIncludedTermPayloadOffset, 8));
        _ = await Assert.That(termOnDisk).IsEqualTo(1UL);

        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount + SnapshotTestLayout.LastIncludedTermPayloadOffset, 8), 0UL);
        var compute = Crc32C.Compute(bytes.AsSpan(SnapshotTestLayout.HeaderByteCount, payloadLength));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(SnapshotTestLayout.CrcFileOffset(payloadLength), 4), compute);
        await File.WriteAllBytesAsync(snapshotPath, bytes, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        var exception = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(exception.Message).Contains("included term is zero", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>
    /// Recovery after a crash between snapshot publication and the installation log rewrite treats the walked,
    /// shorter journal as covered by the published snapshot instead of failing readiness on every start.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotCoversShortJournalPostCrash(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-crash-recovery");
        var composition = GroupComposition.Create(GroupId);
        var store = new GroupSnapshotStore(dir, GroupId);

        // Simulate the crash window: the snapshot at index three and the advanced install-candidate metadata
        // are durable, while the log rewrite never ran and the file still ends at index one.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(cancellationToken);
            _ = await seed.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await seed.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await store.PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 3UL, 3UL, Array.Empty<GroupIdempotencyRecord>()), cancellationToken);

        var candidate = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, string.Empty, 3UL, 3UL, 3UL);
        await WriteMetadataAsync(dir, candidate, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);

        // The watermarks describe the installed snapshot; the journal honestly ends below them until
        // replication rebuilds the covered span from the leader.
        _ = await Assert.That(status.CommitIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>Snapshot encoding rejects a null committed-outcomes list with the documented argument exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotEncodingRejectsNullOutcomes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snapshot-null-outcomes");
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
            await seed.OpenAsync(cancellationToken);

        var nullOutcomes = default(GroupSnapshot) with { GroupId = GroupId };
        var store = new GroupSnapshotStore(dir, GroupId);
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentNullException>(store.PublishAsync(nullOutcomes, cancellationToken));
    }

    /// <summary>A snapshot containing an unresolved outcome is rejected before publication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotWithUnresolvedOutcomeIsRejected(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unresolved-snapshot");
        var composition = GroupComposition.Create(GroupId);

        await using (var seed = new FollowerLog(dir, GroupId, composition))
            await seed.OpenAsync(cancellationToken);

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

        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidOperationException>(store.PublishAsync(snapshot, cancellationToken));
        _ = await Assert.That(store.SnapshotExists).IsFalse();
    }

    /// <summary>Snapshot-only startup validates the published snapshot instead of treating the directory as fresh state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartupRejectsForeignGroupSnapshot(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-snapshot-only-source");
        using var targetDir = new TempDirectory("squirix-snapshot-only-target");
        const string sourceGroupId = "grp-snapshot-source";
        var targetComposition = GroupComposition.Create(GroupId);

        Directory.CreateDirectory(targetDir.Path);

        await using (var source = new FollowerLog(sourceDir, sourceGroupId, GroupComposition.Create(sourceGroupId)))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, 1UL, "snapshot"), cancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, cancellationToken);
        }

        Directory.CreateDirectory(Path.GetDirectoryName(GroupStoragePaths.GetSnapshotPath(targetDir, GroupId))!);
        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, sourceGroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, targetComposition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>The store rejects a published snapshot whose embedded group id differs from its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoreRejectsSnapshotFromAnotherGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-store-wrong-group");

        // The log seed creates the on-disk replication layout that PublishAsync writes into.
        await using (var seed = new FollowerLog(dir, "grp-a", GroupComposition.Create("grp-a")))
            await seed.OpenAsync(cancellationToken);

        var storeA = new GroupSnapshotStore(dir, "grp-a");
        await storeA.PublishAsync(new GroupSnapshot("grp-a", ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>()), cancellationToken);

        var sourcePath = GroupStoragePaths.GetSnapshotPath(dir, "grp-a");
        var targetPath = GroupStoragePaths.GetSnapshotPath(dir, "grp-b");
        Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);
        File.Copy(sourcePath, targetPath);

        var storeB = new GroupSnapshotStore(dir, "grp-b");
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(storeB.ReadPublishedAsync(cancellationToken));

        _ = await Assert.That(exception.Message).Contains("different group", StringComparison.Ordinal);
    }

    /// <summary>
    /// Recovery refuses a published snapshot whose topology fingerprint conflicts with the durable metadata,
    /// failing readiness without restoring outcomes or watermarks from the incompatible snapshot.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TopologyConflictFailsReadiness(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-conflicting-topology-source");
        using var targetDir = new TempDirectory("squirix-conflicting-topology-target");
        var composition = GroupComposition.Create(GroupId);
        var fingerprint = new byte[] { 1, 2, 3, 4 };

        await using (var seed = new FollowerLog(sourceDir, GroupId, composition))
            await seed.OpenAsync(cancellationToken);

        await WriteMetadataAsync(sourceDir, new GroupLogMetadata(GroupId, fingerprint, 2UL, 0UL, string.Empty, 0UL, 0UL, 0UL), cancellationToken);

        await using (var source = new FollowerLog(sourceDir, GroupId, composition))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, 1UL, "snapshot"), cancellationToken);
            _ = source.Idempotency.Reserve("client", "operation-1", [1, 2, 3], GroupRecordKind.UserMutation, 1UL, 1UL);
            _ = source.Idempotency.TryResolve("client", "operation-1", [9], 1UL, 1UL);
            _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, cancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, GroupId, composition))
            await target.OpenAsync(cancellationToken);

        await WriteMetadataAsync(targetDir, new GroupLogMetadata(GroupId, new byte[] { 5, 6, 7, 8 }, 2UL, 0UL, string.Empty, 0UL, 0UL, 0UL), cancellationToken);

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, GroupId), GroupStoragePaths.GetSnapshotPath(targetDir, GroupId));

        await using var reopened = new FollowerLog(targetDir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var reopenedStatus = await reopened.GetStatusAsync(cancellationToken);
        await SequenceAssert.Equal<byte>([5, 6, 7, 8], reopenedStatus.TopologyFingerprint.ToArray());
        _ = await Assert.That(reopenedStatus.CommitIndex).IsEqualTo(0UL);
        _ = await Assert.That(reopenedStatus.LastAppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "operation-1", [1, 2, 3], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A torn first suffix frame after a compacted prefix can be truncated rather than crashing recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornFirstSuffixFrameCanBeTruncated(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-torn-suffix");
        var composition = GroupComposition.Create(GroupId);
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await log.AppendAsync(Append(4UL, "d"), cancellationToken);
            _ = await log.AppendAsync(Append(5UL, "e"), cancellationToken);
            _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
            _ = await log.CreateSnapshotAsync(3UL, cancellationToken);
            var result = await log.CompactAsync(cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();
        }

        var bytes = await File.ReadAllBytesAsync(logPath, cancellationToken);

        // Skip the file header and the first frame's preamble; the four overwritten bytes are the frame's
        // body-length field, so the frame is read as torn rather than as a corrupted body.
        var corruptionOffset = GroupLogCodec.LogFileHeader.Length + GroupLogCodec.FramePreambleByteCount;
        _ = await Assert.That(bytes.Length > corruptionOffset + sizeof(int)).IsTrue();
        bytes[corruptionOffset] = 0x88;
        bytes[corruptionOffset + 1] = 0x13;
        bytes[corruptionOffset + 2] = 0x00;
        bytes[corruptionOffset + 3] = 0x00;
        await File.WriteAllBytesAsync(logPath, bytes, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
    }

    /// <summary>
    /// A torn first suffix frame above a commit watermark that exceeds the snapshot base fails readiness
    /// without truncating the journal, so the committed frames stay on disk for repair.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornSuffixAboveCommitMarkFails(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-torn-suffix-committed");
        var composition = GroupComposition.Create(GroupId);
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
            _ = await log.CreateSnapshotAsync(3UL, cancellationToken);
            var result = await log.CompactAsync(cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();

            // Append and commit past the boundary so the durable commit watermark exceeds the
            // snapshot base while the suffix frames stay outside the snapshot's coverage.
            _ = await log.AppendAsync(Append(4UL, "d"), cancellationToken);
            _ = await log.AppendAsync(Append(5UL, "e"), cancellationToken);
            _ = await log.AdvanceCommitAsync(5UL, cancellationToken);
        }

        // The durable commit watermark (five) now sits above the compaction boundary (three).
        _ = await Assert.That(await ReadDurableCommitIndexAsync(dir, cancellationToken)).IsEqualTo(5UL);

        var bytes = await File.ReadAllBytesAsync(logPath, cancellationToken);

        // Same torn-shape corruption as the truncatable case: overwrite the first suffix frame's
        // body-length field so the frame reads as torn rather than as a corrupted body.
        var corruptionOffset = GroupLogCodec.LogFileHeader.Length + GroupLogCodec.FramePreambleByteCount;
        _ = await Assert.That(bytes.Length > corruptionOffset + sizeof(int)).IsTrue();
        bytes[corruptionOffset] = 0x88;
        bytes[corruptionOffset + 1] = 0x13;
        bytes[corruptionOffset + 2] = 0x00;
        bytes[corruptionOffset + 3] = 0x00;
        await File.WriteAllBytesAsync(logPath, bytes, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        // Recovery must refuse destructively rewriting the journal: the bytes beyond the header
        // remain on disk so the committed frames above the snapshot base can still be repaired.
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That(await ReadDurableCommitIndexAsync(dir, cancellationToken)).IsEqualTo(5UL);
        var survivingBytes = await File.ReadAllBytesAsync(logPath, cancellationToken);
        _ = await Assert.That(survivingBytes.Length > GroupLogCodec.LogFileHeader.Length).IsTrue();
    }

    /// <summary>Unspecified snapshot timestamps round-trip as UTC because the encoder relabels rather than converts them.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnspecifiedTimestampsRoundTripAsUtc(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snapshot-unspecified-time");
        var timestamp = new DateTime(2024, 5, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var outcomes = new List<GroupIdempotencyRecord>
        {
            new("client", "op-1", new byte[] { 1 }, new byte[] { 8 }, GroupRecordKind.UserMutation, timestamp, timestamp, 1UL, 1UL),
        };

        // PublishAsync writes into the on-disk replication layout, so seed it first like production startup does.
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
            await seed.OpenAsync(cancellationToken);

        await new GroupSnapshotStore(dir, GroupId).PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, outcomes), cancellationToken);

        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(cancellationToken)).IsNotNull();
        var restored = await Assert.That(published.CommittedOutcomes).HasSingleItem();
        var expected = new DateTime(2024, 5, 1, 12, 0, 0, DateTimeKind.Utc);
        _ = await Assert.That(restored.CreatedUtc).IsEqualTo(expected);
        _ = await Assert.That(restored.ResolvedUtc).IsEqualTo(expected);
    }

    /// <summary>A snapshot belonging to a different group fails readiness on recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WrongGroupSnapshotFailsReadiness(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-wrong-group-source");
        using var targetDir = new TempDirectory("squirix-wrong-group-target");
        var composition = GroupComposition.Create("grp-a");

        await using (var source = new FollowerLog(sourceDir, "grp-a", composition))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await source.CreateSnapshotAsync(1UL, cancellationToken);
        }

        await using (var target = new FollowerLog(targetDir, "grp-b", GroupComposition.Create("grp-b")))
        {
            await target.OpenAsync(cancellationToken);
            _ = await target.AppendAsync(Append(1UL, "b"), cancellationToken);
            _ = await target.AdvanceCommitAsync(1UL, cancellationToken);
        }

        File.Copy(GroupStoragePaths.GetSnapshotPath(sourceDir, "grp-a"), GroupStoragePaths.GetSnapshotPath(targetDir, "grp-b"));

        await using var reopened = new FollowerLog(targetDir, "grp-b", GroupComposition.Create("grp-b"));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);

    /// <summary>Reads the durable metadata's commit index directly from the encoded metadata file.</summary>
    /// <param name="dir">The temporary directory holding the replication layout.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The durable commit index.</returns>
    /// <exception cref="InvalidDataException">Thrown when the encoded metadata fails codec validation.</exception>
    private static async Task<ulong> ReadDurableCommitIndexAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        var encoded = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        return GroupLogCodec.TryDecodeMeta(encoded, out var meta) ? meta.CommitIndex : throw new InvalidDataException("The durable metadata payload failed codec validation.");
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
}
