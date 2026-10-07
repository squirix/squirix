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
    private const ulong Applied = 2UL;
    private const ulong Committed = 4UL;
    private const string GroupId = "grp-compaction";
    private const ulong LastIndex = 5UL;

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

    /// <summary>
    /// A log whose commit index runs ahead of its applied index compacts through the applied index and keeps every frame above it: the
    /// committed entries still to apply and the uncommitted tail.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactThroughAppliedKeepsCommittedTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-committed-tail");
        await using var log = await SeedCommittedTailAsync(dir, null, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Applied, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);

        var retention = await RetentionAsync(log, cancellationToken);
        _ = await Assert.That((retention.SnapshotIndex, retention.RetainedEntries, retention.RetainedPayloads)).IsEqualTo((Applied, 3, 3));
        await AssertCommittedTailAsync(log, cancellationToken);
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(Applied);
    }

    /// <summary>Only the applied index compacts: an index below it, a committed index above it, and an uncommitted index are refused, and nothing changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactRefusesIndexOtherThanApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-other-index");
        await using var log = await SeedCommittedTailAsync(dir, null, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Applied - 1, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);
        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);
        _ = await Assert.That(await log.CompactThroughAsync(LastIndex, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);

        var retention = await RetentionAsync(log, cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(log.SnapshotPath).IsNull();
        _ = await Assert.That((retention.SnapshotIndex, retention.RetainedEntries)).IsEqualTo((0UL, int.CreateChecked(LastIndex)));
    }

    /// <summary>
    /// A restart after compacting through the applied index recovers the snapshot, the commit index above it, and every retained frame, and
    /// the log compacts again once the retained committed entries are applied.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryKeepsCommittedTailAboveSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-committed-tail-restart");
        await using (var log = await SeedCommittedTailAsync(dir, null, cancellationToken))
            _ = await Assert.That(await log.CompactThroughAsync(Applied, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        var retention = await RetentionAsync(reopened, cancellationToken);
        _ = await Assert.That((retention.SnapshotIndex, retention.RetainedEntries)).IsEqualTo((Applied, 3));
        await AssertCommittedTailAsync(reopened, cancellationToken);

        _ = await reopened.AdvanceAppliedAsync(Committed, cancellationToken);
        _ = await Assert.That(await reopened.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((LastIndex, Committed, Committed));
        _ = await Assert.That((await reopened.GetUncommittedTailAsync(cancellationToken))[0].LogIndex).IsEqualTo(LastIndex);
    }

    /// <summary>
    /// A failure at either durability boundary of a compaction through the applied index leaves a log that reopens with the committed tail
    /// above the snapshot.
    /// </summary>
    /// <param name="boundary">The boundary the failure is injected at.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(GroupLogCompactThroughTests.FaultBoundary.AfterSnapshotPublish)]
    [Arguments(GroupLogCompactThroughTests.FaultBoundary.AfterLogReplace)]
    public async Task FaultKeepsCommittedTail(GroupLogCompactThroughTests.FaultBoundary boundary, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-committed-tail-fault");
        var armed = false;
        var hooks = new IFollowerLogFaultHooksCreateExpectations();
        _ = hooks.Setups.OnFrameWritten();
        _ = hooks.Setups.OnCommitAdvanced();
        _ = hooks.Setups.OnBeforeMemoryApply();
        _ = hooks.Setups.OnFlushed().Callback(() => FailIfArmed(armed && boundary == GroupLogCompactThroughTests.FaultBoundary.AfterSnapshotPublish));
        _ = hooks.Setups.OnMetaWritten().Callback(() => FailIfArmed(armed && boundary == GroupLogCompactThroughTests.FaultBoundary.AfterLogReplace));
        await using (var log = await SeedCommittedTailAsync(dir, new FollowerLogOptions { FaultHooks = hooks.Instance() }, cancellationToken))
        {
            armed = true;
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(Applied, cancellationToken));
            armed = false;
        }

        // The snapshot is published at both boundaries; only the later one swapped in the log that starts right above it.
        var first = await FirstFrameIndexAsync(dir, cancellationToken);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetSnapshotPath(dir, GroupId))).IsTrue();
        _ = await Assert.That(first).IsEqualTo(boundary == GroupLogCompactThroughTests.FaultBoundary.AfterLogReplace ? Applied + 1 : 1UL);

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        await AssertCommittedTailAsync(reopened, cancellationToken);
        _ = await Assert.That(await reopened.CompactThroughAsync(Applied, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        await AssertCommittedTailAsync(reopened, cancellationToken);
    }

    /// <summary>
    /// A store already past its idempotency capacity compacts, and an outcome applied while the snapshot is published keeps the log ready:
    /// committed outcomes are never refused for the capacity, before or after the snapshot is published.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactPastCapacityStaysReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compaction-past-capacity");
        var now = DateTime.UtcNow;
        var armed = new int[1];
        GroupIdempotencyState? store = null;
        var hooks = new IFollowerLogFaultHooksCreateExpectations();
        _ = hooks.Setups.OnFrameWritten();
        _ = hooks.Setups.OnCommitAdvanced();
        _ = hooks.Setups.OnBeforeMemoryApply();
        _ = hooks.Setups.OnMetaWritten();

        // The first flush once armed publishes the snapshot: the outcome recorded there lands between the snapshot and the log rewrite.
        _ = hooks.Setups.OnFlushed().Callback(() =>
        {
            if (Interlocked.Exchange(ref armed[0], 0) == 1)
                store?.RecordCommittedOutcome(CommittedOutcome("applied-3", Applied + 1, now));
        });
        await using var log = await SeedCommittedTailAsync(dir, new FollowerLogOptions { IdempotencyCapacity = 1, FaultHooks = hooks.Instance() }, cancellationToken);
        store = log.Idempotency;
        store.MarkOutcomesRebuilt();
        store.RecordCommittedOutcome(CommittedOutcome("applied-1", 1UL, now));
        store.RecordCommittedOutcome(CommittedOutcome("applied-2", Applied, now));
        Volatile.Write(ref armed[0], 1);

        var outcome = await log.CompactThroughAsync(Applied, cancellationToken);

        var retention = await RetentionAsync(log, cancellationToken);
        _ = await Assert.That((outcome, log.Readiness, retention.SnapshotIndex)).IsEqualTo((GroupCompactionOutcome.Compacted, FollowerLogReadiness.Ready, Applied));
        _ = await Assert.That(Volatile.Read(ref armed[0])).IsEqualTo(0).Because("The outcome must be recorded while the compaction runs.");
        var found = (store.Lookup("client", "applied-1", [1], out _), store.Lookup("client", "applied-2", [1], out _), store.Lookup("client", "applied-3", [1], out _));
        _ = await Assert.That(found).IsEqualTo((GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found));
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
    /// A crash while an installed snapshot rewrites the journal recovers the same state a completed installation leaves: the applied watermark
    /// stays at the snapshot boundary persisted before the rewrite, so no entry the snapshot covers is handed out for application.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallCrashKeepsAppliedAtBoundary(CancellationToken cancellationToken)
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
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL, 3UL));
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).IsEmpty();
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

    private static GroupIdempotencyRecord CommittedOutcome(string operationId, ulong logIndex, DateTime decidedUtc) =>
        new("client", operationId, new byte[] { 1 }, new byte[] { 8 }, GroupRecordKind.UserMutation, decidedUtc, decidedUtc, logIndex, 1UL);

    private static void FailIfArmed(bool armed)
    {
        if (armed)
            throw new IOException("simulated crash at a compaction boundary.");
    }

    private static string Payload(ulong index) => "value-" + index;

    private static ValueTask<FollowerLogRetention> RetentionAsync(IFollowerLog log, CancellationToken cancellationToken) => log.GetRetentionAsync(cancellationToken);

    /// <summary>Asserts the state a compaction through the applied index leaves, with the committed tail and the uncommitted entry retained.</summary>
    /// <param name="log">The log to check.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertCommittedTailAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((LastIndex, Committed, Applied));

        var committed = await log.GetCommittedEntriesAsync(Applied, 10, cancellationToken);
        _ = await Assert.That(committed.Count).IsEqualTo(int.CreateChecked(Committed - Applied));
        for (var i = 0; i < committed.Count; i++)
        {
            var index = Applied + 1 + ulong.CreateChecked(i);
            _ = await Assert.That((committed[i].LogIndex, Encoding.UTF8.GetString(committed[i].Payload.Span))).IsEqualTo((index, Payload(index)));
        }

        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That((tail[0].LogIndex, Encoding.UTF8.GetString(tail[0].Payload.Span))).IsEqualTo((LastIndex, Payload(LastIndex)));
    }

    /// <summary>Reads the log index of the first frame of the durable group log.</summary>
    /// <param name="dir">The node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The first frame's log index, or <c language="csharp">0</c> when no frame opens the log.</returns>
    private static async Task<ulong> FirstFrameIndexAsync(string dir, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetLogPath(dir, GroupId), cancellationToken);
        return GroupLogCodec.TryReadFrameFields(bytes.AsSpan(GroupLogCodec.LogFileHeader.Length), out var logIndex, out _) ? logIndex : 0UL;
    }

    /// <summary>Opens a log holding entries 1 to 5, committed through 4 and applied through 2.</summary>
    /// <param name="dir">The node data directory.</param>
    /// <param name="options">The log options, or <see langword="null" /> for the defaults.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The open log.</returns>
    private static async Task<FollowerLog> SeedCommittedTailAsync(string dir, FollowerLogOptions? options, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options);
        try
        {
            await log.OpenAsync(cancellationToken);
            for (var index = 1UL; index <= LastIndex; index++)
                _ = await log.AppendAsync(Append(index, Payload(index)), cancellationToken);

            _ = await log.AdvanceCommitAsync(Committed, cancellationToken);
            _ = await log.AdvanceAppliedAsync(Applied, cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);
}
