using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Compaction behavior that retains an installable replica-group snapshot.</summary>
public sealed class ReplicaCompactionTests : ServerUnitTestBase
{
    private const string GroupId = "grp-compaction";

    /// <summary>
    /// Compaction refuses an index below the applied watermark, so committed-and-applied frames are never dropped by a snapshot that does not cover them, and changes nothing.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRefusesBelowAppliedMark(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-below-applied");
        var composition = GroupComposition.Create(GroupId);

        await using (var seed = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance))
        {
            await seed.OpenAsync(cancellationToken);
            _ = await seed.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await seed.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await seed.AppendAsync(Append(3UL, "c"), cancellationToken);
        }

        // Durable metadata that carries an applied watermark above the commit index: the exact incoherent state the guard defends against,
        // where compacting through the commit index would otherwise drop applied frames.
        var incoherent = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 3UL, 2UL, 3UL);
        var encoded = new byte[GroupLogCodec.ComputeMetaEncodedLength(in incoherent)];
        GroupLogCodec.EncodeMeta(in incoherent, encoded);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), encoded, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);

        _ = await Assert.That(await log.CompactThroughAsync(2UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);

        var after = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(log.SnapshotPath).IsNull();
        _ = await Assert.That((after.CommitIndex, after.LastLogIndex, after.LastAppliedIndex)).IsEqualTo((2UL, 3UL, 3UL));
        _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
    }

    /// <summary>A failed replacement after flushing the compacted file preserves the original durable journal and the readable published snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedReplacementPreservesDurableJournal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-compaction-replacement-fault");
        var faults = FollowerSnapshotScenario.CreateCompactionFaults();
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            await FollowerSnapshotScenario.PublishWithoutCompactionAsync(log, faults, 2UL, cancellationToken);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            var tempPath = GroupStoragePaths.GetLogTempPath(dir, GroupId);
            _ = await Assert.That(File.Exists(tempPath)).IsFalse().Because($"Compaction temp file should be cleaned up after failure: {tempPath}");
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
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

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 3UL, cancellationToken);

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

        var faults = FollowerSnapshotScenario.CreateCompactionFaults();
        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, "a"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AppendAsync(Append(2UL, "b"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();
            _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);

            // A newer snapshot is published without compacting the journal again, so the durable journal still
            // starts at the previous compaction boundary plus one while the snapshot covers through index three.
            _ = await Assert.That((await log.AppendAsync(Append(3UL, "c"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(3UL, cancellationToken)).Success).IsTrue();
            await FollowerSnapshotScenario.PublishWithoutCompactionAsync(log, faults, 3UL, cancellationToken);
        }

        // The restart lands on the third journal shape: the first frame lies above one and below snapshotBase + one.
        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL));

        // The compaction path publishes a snapshot only for an applied prefix, so no committed entry is left to hand out again.
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).IsEmpty();

        var snapshotStore = new GroupSnapshotStore(dir, GroupId);
        var published = await Assert.That(await snapshotStore.ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(3UL);

        // The recovered log stays usable: it compacts through the snapshot boundary it recovered.
        _ = await Assert.That(await reopened.CompactThroughAsync(3UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
    }

    /// <summary>
    /// A crash while an installed snapshot rewrites the journal leaves a log whose snapshot covers committed entries the applied watermark has not reached;
    /// recovery keeps handing those entries out for application.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallCrashKeepsUnappliedEntries(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-install-restart");
        using var sourceDir = new TempDirectory("squirix-compaction-install-restart-source");
        var composition = GroupComposition.Create(GroupId);

        GroupSnapshot snapshot;
        await using (var source = new FollowerLog(sourceDir, GroupId, composition, NullLogger<FollowerLog>.Instance))
        {
            await source.OpenAsync(cancellationToken);
            _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await source.AdvanceCommitAsync(3UL, cancellationToken);
            snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, 3UL, cancellationToken);
        }

        var faults = FollowerSnapshotScenario.CreateCompactionFaults();
        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(3UL, cancellationToken);

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken));
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL, 2UL));
        var committed = await reopened.GetCommittedEntriesAsync(cancellationToken);
        var single = await Assert.That(committed).HasSingleItem();
        _ = await Assert.That(Encoding.UTF8.GetString(single.Payload.Span)).IsEqualTo("c");
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(3UL);
    }

    /// <summary>Compaction retains the snapshot and recovers the uncommitted tail after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainsInstallableStateForLaggingReplica(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-compaction");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);

            _ = await Assert.That(log.SnapshotPath).IsNotNull();
            _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).HasSingleItem();
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
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
}
