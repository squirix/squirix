using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Compaction behavior that retains an installable replica-group snapshot.</summary>
public sealed class ReplicaCompactionTests : ServerUnitTestBase
{
    private const string GroupId = "grp-compaction";

    /// <summary>Compaction refuses a published snapshot whose boundary term conflicts with the local boundary, preserving the divergent suffix as a replicable tail.</summary>
    [Fact]
    public async Task CompactionRefusesConflictingBoundaryTerm()
    {
        using var dir = new TempDirectory("squirix-compaction-divergent-boundary");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken);

        // Publish a snapshot whose included term diverges from the local boundary entry's term. Compaction refuses the
        // snapshot and preserves the divergent suffix as a replicable tail.
        var store = new GroupSnapshotStore(dir, GroupId);
        var divergent = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 99UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        await store.PublishAsync(divergent, DefaultCancellationToken);

        var result = await log.CompactAsync(DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, result.RefusalCode);
        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, status.LastLogIndex);
        _ = Assert.Single(await log.GetUncommittedTailAsync(DefaultCancellationToken));
    }

    /// <summary>Compaction refuses a published snapshot whose committed outcomes exceed the configured idempotency capacity, so the durable prefix is never truncated without a restorable state.</summary>
    [Fact]
    public async Task CompactionRefusesSnapshotPastCapacity()
    {
        using var dir = new TempDirectory("squirix-compaction-capacity");
        var composition = GroupComposition.Create(GroupId);
        var options = new FollowerLogOptions { IdempotencyCapacity = 2 };

        var now = DateTime.UtcNow;
        var outcomes = new GroupIdempotencyRecord[]
        {
            new("leader", "op-1", ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, GroupRecordKind.UserMutation, now, now, 1UL, 1UL),
            new("leader", "op-2", ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, GroupRecordKind.UserMutation, now, now, 1UL, 1UL),
            new("leader", "op-3", ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, GroupRecordKind.UserMutation, now, now, 2UL, 1UL),
        };
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        byte[] bytesBeforeCompaction;
        byte[] bytesAfterCompaction;
        await using (var log = new FollowerLog(dir, GroupId, composition, options))
        {
            await log.OpenAsync(DefaultCancellationToken);
            Assert.True((await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken)).Success);
            Assert.True((await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceCommitAsync(2UL, DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken)).Success);

            // Published snapshot covers both committed entries but exports three distinct resolved outcomes,
            // more than the configured capacity of two. Compaction must refuse before the destructive rewrite.
            var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, outcomes);
            await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, DefaultCancellationToken);

            bytesBeforeCompaction = await ReadLogBytesAsync(logPath);
            var result = await log.CompactAsync(DefaultCancellationToken);
            bytesAfterCompaction = await ReadLogBytesAsync(logPath);

            Assert.False(result.Success);
            Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        }

        // The durable prefix must survive intact because compaction refused before any truncate: the journal
        // bytes are unchanged and a reopened log still serves the committed entries.
        Assert.Equal(bytesBeforeCompaction, bytesAfterCompaction);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        // The applied prefix releases its payloads during recovery, so the watermarks — not the entry payloads —
        // prove the journal survived the refused compaction.
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, status.LastLogIndex);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(2UL, status.LastAppliedIndex);
    }

    /// <summary>Compaction refuses a published snapshot whose included boundary falls below the applied watermark, so committed-and-applied frames are never dropped without a covering snapshot.</summary>
    [Fact]
    public async Task CompactionRefusesBelowAppliedMark()
    {
        using var dir = new TempDirectory("squirix-compaction-below-applied");
        var composition = GroupComposition.Create(GroupId);

        // Seed a log whose durable metadata carries an applied watermark above the snapshot's included boundary:
        // the exact incoherent state the guard defends against, where compaction would otherwise drop applied frames.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(DefaultCancellationToken);
            _ = await seed.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await seed.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
            _ = await seed.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
            _ = await seed.AdvanceCommitAsync(2UL, DefaultCancellationToken);
            _ = await seed.AdvanceAppliedAsync(2UL, DefaultCancellationToken);
            _ = await seed.CreateSnapshotAsync(2UL, DefaultCancellationToken);
        }

        var incoherent = new GroupLogMetadata(
            GroupId,
            ReadOnlyMemory<byte>.Empty,
            0UL,
            0UL,
            string.Empty,
            3UL,
            2UL,
            3UL);
        var encoded = new byte[GroupLogCodec.ComputeMetaEncodedLength(incoherent)];
        GroupLogCodec.EncodeMeta(incoherent, encoded);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), encoded, DefaultCancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);

        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(3UL, status.LastAppliedIndex);
        Assert.Equal(3UL, status.LastLogIndex);

        // Published snapshot covers only index 2 while the applied watermark sits at 3.
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, DefaultCancellationToken);

        var result = await log.CompactAsync(DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        var after = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(2UL, after.CommitIndex);
        Assert.Equal(3UL, after.LastLogIndex);
        Assert.Equal(3UL, after.LastAppliedIndex);
        Assert.Empty(await log.GetUncommittedTailAsync(DefaultCancellationToken));
    }

    /// <summary>Compaction releases an unresolved reservation that the snapshot did not export, and keeps the resolved outcome.</summary>
    [Fact]
    public async Task CompactionReleasesPrefixReservation()
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-compaction");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        Assert.Equal(
            GroupIdempotencyReserveResult.Success,
            log.Idempotency.Reserve("client", "orphan", new byte[] { 1 }, GroupRecordKind.UserMutation, 2UL, 1UL));
        Assert.Equal(
            GroupIdempotencyReserveResult.Success,
            log.Idempotency.Reserve("client", "kept", new byte[] { 2 }, GroupRecordKind.UserMutation, 1UL, 1UL));
        Assert.True(log.Idempotency.TryResolve("client", "kept", new byte[] { 3 }, 1UL, 1UL));
        _ = await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken);
        var snapshot = await log.CreateSnapshotAsync(2UL, DefaultCancellationToken);
        Assert.Equal(GroupId, snapshot.GroupId);
        var compaction = await log.CompactAsync(DefaultCancellationToken);
        Assert.True(compaction.Success);

        Assert.Equal(GroupIdempotencyLookup.Miss, log.Idempotency.Lookup("client", "orphan", new byte[] { 1 }, out _));
        Assert.Equal(GroupIdempotencyLookup.Found, log.Idempotency.Lookup("client", "kept", new byte[] { 2 }, out var record));
        Assert.True(record.IsResolved);
        Assert.Equal(GroupIdempotencyReserveResult.Success, log.Idempotency.Reserve("client", "orphan", new byte[] { 1 }, GroupRecordKind.UserMutation, 3UL, 1UL));
    }

    /// <summary>A failed replacement after flushing the compacted file preserves the original durable journal and the readable published snapshot.</summary>
    [Fact]
    public async Task FailedReplacementPreservesDurableJournal()
    {
        using var dir = new TempDirectory("squirix-replica-compaction-replacement-fault");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated failure before compaction publish."));
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, faults))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
            _ = await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken);
            _ = await log.CreateSnapshotAsync(2UL, DefaultCancellationToken);

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactAsync(DefaultCancellationToken));
            Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);
            var tempPath = GroupStoragePaths.GetLogTempPath(dir, GroupId);
            Assert.False(File.Exists(tempPath), $"Compaction temp file should be cleaned up after failure: {tempPath}");
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(3UL, status.LastLogIndex);
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        _ = Assert.Single(tail);
        Assert.Equal(3UL, tail[0].LogIndex);
        Assert.Equal("c", Encoding.UTF8.GetString(tail[0].Payload.Span));

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = Assert.NotNull(await snapshotStore.ReadPublishedAsync(DefaultCancellationToken));
        Assert.Equal(2UL, published.LastIncludedIndex);
    }

    /// <summary>Probing below the snapshot boundary after compaction returns a log mismatch without failing readiness.</summary>
    [Fact]
    public async Task ProbeBelowBoundaryReturnsMismatch()
    {
        using var dir = new TempDirectory("squirix-compaction-probe");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, DefaultCancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, DefaultCancellationToken);
        _ = await log.CreateSnapshotAsync(3UL, DefaultCancellationToken);
        var compaction = await log.CompactAsync(DefaultCancellationToken);
        Assert.True(compaction.Success);

        var probe = new FollowerLogAppendRequest("leader", 1UL, 2UL, 1UL, 3UL, default);
        var result = await log.AppendAsync(probe, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
    }

    /// <summary>Compaction retains the snapshot and recovers the uncommitted tail after restart.</summary>
    [Fact]
    public async Task RetainsInstallableStateForLaggingReplica()
    {
        using var dir = new TempDirectory("squirix-replica-compaction");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
            _ = await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken);
            _ = await log.CreateSnapshotAsync(2UL, DefaultCancellationToken);

            var result = await log.CompactAsync(DefaultCancellationToken);

            Assert.True(result.Success);
            Assert.NotNull(result.SnapshotPath);
            _ = Assert.Single(await log.GetUncommittedTailAsync(DefaultCancellationToken));
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(2UL, status.CommitIndex);
        Assert.Equal(3UL, status.LastLogIndex);
        Assert.Equal("c", Encoding.UTF8.GetString((await reopened.GetUncommittedTailAsync(DefaultCancellationToken))[0].Payload.ToArray()));

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = Assert.NotNull(await snapshotStore.ReadPublishedAsync(DefaultCancellationToken));
        Assert.Equal(2UL, published.LastIncludedIndex);
    }

    /// <summary>
    /// Recovery opens a journal whose first frame starts inside a newer snapshot's covered prefix: the crash
    /// between publishing that snapshot and the next compaction leaves valid durable state that must not be
    /// reported as a committed gap.
    /// </summary>
    [Fact]
    public async Task RecoversJournalInsideNewerSnapshotPrefix()
    {
        using var dir = new TempDirectory("squirix-compaction-snapshot-restart");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(DefaultCancellationToken);
            Assert.True((await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken)).Success);
            Assert.True((await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceCommitAsync(2UL, DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceAppliedAsync(2UL, DefaultCancellationToken)).Success);

            _ = await log.CreateSnapshotAsync(2UL, DefaultCancellationToken);
            var compaction = await log.CompactAsync(DefaultCancellationToken);
            Assert.True(compaction.Success);

            // A newer snapshot is published without compacting the journal again, so the durable journal still
            // starts at the previous compaction boundary plus one while the snapshot covers through index three.
            Assert.True((await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceCommitAsync(3UL, DefaultCancellationToken)).Success);
            var snapshot = await log.CreateSnapshotAsync(3UL, DefaultCancellationToken);
            Assert.Equal(3UL, snapshot.LastIncludedIndex);
        }

        // The restart lands on the third journal shape: the first frame lies above one and below snapshotBase + one.
        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, status.LastLogIndex);
        var committed = await reopened.GetCommittedEntriesAsync(DefaultCancellationToken);
        _ = Assert.Single(committed);
        Assert.Equal("c", Encoding.UTF8.GetString(committed[0].Payload.Span));

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = Assert.NotNull(await snapshotStore.ReadPublishedAsync(DefaultCancellationToken));
        Assert.Equal(3UL, published.LastIncludedIndex);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);

    /// <summary>Reads the durable journal bytes while the durability layer holds the log open.</summary>
    /// <param name="path">The journal file path to read.</param>
    /// <returns>The complete journal content.</returns>
    /// <exception cref="InvalidOperationException">The journal could not be read in full.</exception>
    private static async Task<byte[]> ReadLogBytesAsync(string path)
    {
        // The durability layer holds the log open with FileShare.Read, so a share-compatible handle is required.
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var content = new byte[RandomAccess.GetLength(handle)];
        return await HandleEx.TryReadExactAsync(handle, content, 0, DefaultCancellationToken).ConfigureAwait(false) != null
            ? content
            : throw new InvalidOperationException($"Incomplete read of '{path}'.");
    }
}
