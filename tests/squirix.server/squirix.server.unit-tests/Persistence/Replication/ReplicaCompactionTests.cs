using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Compaction behavior that retains an installable replica-group snapshot.</summary>
public sealed class ReplicaCompactionTests : ServerUnitTestBase
{
    private const string GroupId = "grp-compaction";

    /// <summary>
    /// Compaction refuses a published snapshot whose included boundary falls below the applied watermark, so committed-and-applied frames are never dropped without a covering
    /// snapshot.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRefusesBelowAppliedMark(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-below-applied");
        var composition = GroupComposition.Create(GroupId);

        // Seed a log whose durable metadata carries an applied watermark above the snapshot's included boundary:
        // the exact incoherent state the guard defends against, where compaction would otherwise drop applied frames.
        await using (var seed = new FollowerLog(dir, GroupId, composition))
        {
            await seed.OpenAsync(cancellationToken);
            _ = await seed.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await seed.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await seed.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await seed.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await seed.AdvanceAppliedAsync(2UL, cancellationToken);
            _ = await seed.CreateSnapshotAsync(2UL, cancellationToken);
        }

        var incoherent = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 3UL, 2UL, 3UL);
        var encoded = new byte[GroupLogCodec.ComputeMetaEncodedLength(incoherent)];
        GroupLogCodec.EncodeMeta(incoherent, encoded);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), encoded, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);

        // Published snapshot covers only index 2 while the applied watermark sits at 3.
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken);

        var result = await log.CompactAsync(cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        var after = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(after.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(after.LastAppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
    }

    /// <summary>Compaction refuses a published snapshot whose boundary term conflicts with the local boundary, preserving the divergent suffix as a replicable tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRefusesConflictingBoundaryTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-divergent-boundary");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);

        // Publish a snapshot whose included term diverges from the local boundary entry's term. Compaction refuses the
        // snapshot and preserves the divergent suffix as a replicable tail.
        var store = new GroupSnapshotStore(dir, GroupId);
        var divergent = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 99UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>());
        await store.PublishAsync(divergent, cancellationToken);

        var result = await log.CompactAsync(cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).HasSingleItem();
    }

    /// <summary>
    /// Compaction refuses a published snapshot whose committed outcomes exceed the configured idempotency capacity, so the durable prefix is never truncated without a restorable
    /// state.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRefusesSnapshotPastCapacity(CancellationToken cancellationToken)
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
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, "a"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AppendAsync(Append(2UL, "b"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceAppliedAsync(2UL, cancellationToken)).Success).IsTrue();

            // Published snapshot covers both committed entries but exports three distinct resolved outcomes,
            // more than the configured capacity of two. Compaction must refuse before the destructive rewrite.
            var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, outcomes);
            await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken);

            bytesBeforeCompaction = await ReadLogBytesAsync(logPath, cancellationToken);
            var result = await log.CompactAsync(cancellationToken);
            bytesAfterCompaction = await ReadLogBytesAsync(logPath, cancellationToken);

            _ = await Assert.That(result.Success).IsFalse();
            _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        }

        // The durable prefix must survive intact because compaction refused before any truncate: the journal
        // bytes are unchanged and a reopened log still serves the committed entries.
        await SequenceAssert.Equal(bytesBeforeCompaction, bytesAfterCompaction);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        // The applied prefix releases its payloads during recovery, so the watermarks — not the entry payloads —
        // prove the journal survived the refused compaction.
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(2UL);
    }

    /// <summary>Compaction releases an unresolved reservation that the snapshot did not export, and keeps the resolved outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionReleasesPrefixReservation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-compaction");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.Reserve("client", "kept", [2], GroupRecordKind.UserMutation, 1UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.TryResolve("client", "kept", [3], 1UL, 1UL)).IsTrue();
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
        var snapshot = await log.CreateSnapshotAsync(2UL, cancellationToken);
        _ = await Assert.That(snapshot.GroupId).IsEqualTo(GroupId);
        var compaction = await log.CompactAsync(cancellationToken);
        _ = await Assert.That(compaction.Success).IsTrue();

        _ = await Assert.That(log.Idempotency.Lookup("client", "orphan", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(log.Idempotency.Lookup("client", "kept", [2], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.IsResolved).IsTrue();
        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 3UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>A failed replacement after flushing the compacted file preserves the original durable journal and the readable published snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedReplacementPreservesDurableJournal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-compaction-replacement-fault");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated failure before compaction publish."));
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
            _ = await log.CreateSnapshotAsync(2UL, cancellationToken);

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactAsync(cancellationToken));
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            var tempPath = GroupStoragePaths.GetLogTempPath(dir, GroupId);
            _ = await Assert.That(File.Exists(tempPath)).IsFalse().Because($"Compaction temp file should be cleaned up after failure: {tempPath}");
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[0].Payload.Span)).IsEqualTo("c");

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = await Assert.That(await snapshotStore.ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(2UL);
    }

    /// <summary>Probing below the snapshot boundary after compaction returns a log mismatch without failing readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProbeBelowBoundaryReturnsMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-probe");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
        _ = await log.CreateSnapshotAsync(3UL, cancellationToken);
        var compaction = await log.CompactAsync(cancellationToken);
        _ = await Assert.That(compaction.Success).IsTrue();

        var probe = new FollowerLogAppendRequest("leader", 1UL, 2UL, 1UL, 3UL, default);
        var result = await log.AppendAsync(probe, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    /// <summary>
    /// Recovery opens a journal whose first frame starts inside a newer snapshot's covered prefix: the crash
    /// between publishing that snapshot and the next compaction leaves valid durable state that must not be
    /// reported as a committed gap.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoversJournalInsideNewerSnapshotPrefix(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-snapshot-restart");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, "a"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AppendAsync(Append(2UL, "b"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceAppliedAsync(2UL, cancellationToken)).Success).IsTrue();

            _ = await log.CreateSnapshotAsync(2UL, cancellationToken);
            var compaction = await log.CompactAsync(cancellationToken);
            _ = await Assert.That(compaction.Success).IsTrue();

            // A newer snapshot is published without compacting the journal again, so the durable journal still
            // starts at the previous compaction boundary plus one while the snapshot covers through index three.
            _ = await Assert.That((await log.AppendAsync(Append(3UL, "c"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(3UL, cancellationToken)).Success).IsTrue();
            var snapshot = await log.CreateSnapshotAsync(3UL, cancellationToken);
            _ = await Assert.That(snapshot.LastIncludedIndex).IsEqualTo(3UL);
        }

        // The restart lands on the third journal shape: the first frame lies above one and below snapshotBase + one.
        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        var committed = await reopened.GetCommittedEntriesAsync(cancellationToken);
        _ = await Assert.That(committed).HasSingleItem();
        _ = await Assert.That(Encoding.UTF8.GetString(committed[0].Payload.Span)).IsEqualTo("c");

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = await Assert.That(await snapshotStore.ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(3UL);
    }

    /// <summary>Compaction retains the snapshot and recovers the uncommitted tail after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainsInstallableStateForLaggingReplica(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-compaction");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
            _ = await log.CreateSnapshotAsync(2UL, cancellationToken);

            var result = await log.CompactAsync(cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That(result.SnapshotPath).IsNotNull();
            _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).HasSingleItem();
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(Encoding.UTF8.GetString((await reopened.GetUncommittedTailAsync(cancellationToken))[0].Payload.ToArray())).IsEqualTo("c");

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = await Assert.That(await snapshotStore.ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(2UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);

    /// <summary>Reads the durable journal bytes while the durability layer holds the log open.</summary>
    /// <param name="path">The journal file path to read.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The complete journal content.</returns>
    /// <exception cref="InvalidOperationException">The journal could not be read in full.</exception>
    private static async Task<byte[]> ReadLogBytesAsync(string path, CancellationToken cancellationToken)
    {
        // The durability layer holds the log open with FileShare.Read, so a share-compatible handle is required.
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var content = new byte[RandomAccess.GetLength(handle)];
        return await HandleEx.ReadExactAsync(handle, content, 0, cancellationToken).ConfigureAwait(false) != null ? content
            : throw new InvalidOperationException($"Incomplete read of '{path}'.");
    }
}
