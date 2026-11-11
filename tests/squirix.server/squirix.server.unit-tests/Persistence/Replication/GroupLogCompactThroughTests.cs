using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Gated compaction of a replica group log through its committed and applied index.</summary>
public sealed class GroupLogCompactThroughTests : ServerUnitTestBase
{
    private const ulong Committed = 3UL;
    private const int EntryCount = 3;
    private const string GroupId = "grp-compact-through";

    /// <summary>The first compaction retains the entire WAL as a fallback while publishing the snapshot baseline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstCompactionRetainsFullWal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through");
        await using (var log = await SeedAsync(dir, null, cancellationToken))
        {
            var before = await RetentionAsync(log, cancellationToken);
            _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);

            var retention = await RetentionAsync(log, cancellationToken);
            _ = await Assert.That(retention).IsEqualTo(new FollowerLogRetention(before.LogBytes, 0, 0, Committed) { CompactionBytes = HeaderLength() });
            await AssertCompactedStateAsync(log, cancellationToken);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        await AssertCompactedStateAsync(reopened, cancellationToken);
    }

    /// <summary>
    /// A failure after the snapshot is published and the rewritten log is flushed to its temp file, before it is swapped in, leaves a log that
    /// reopens to the committed, applied state. The swapped log is the last durable step, so its reopen is the one of a completed compaction.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FaultReopensConsistent(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-fault");
        var armed = false;
        var hooks = new IFollowerLogFaultHooksCreateExpectations();
        _ = hooks.Setups.OnFrameWritten();
        _ = hooks.Setups.OnFramesScheduling();
        _ = hooks.Setups.OnCommitAdvanced();
        _ = hooks.Setups.OnBeforeMemoryApply();
        _ = hooks.Setups.OnFlushed().Callback(() => FailIfArmed(armed));
        _ = hooks.Setups.OnMetaPublishing();
        _ = hooks.Setups.OnSnapshotPublishing();
        await using (var log = await SeedAsync(dir, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks.Instance() }, cancellationToken))
        {
            armed = true;
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(Committed, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            armed = false;
        }

        // The failure hit the intended boundary: the snapshot is published and the log was not swapped.
        var logBytes = new FileInfo(GroupLogFile.ActivePath(dir, GroupId)).Length;
        _ = await Assert.That(GroupSnapshotFile.Exists(dir, GroupId)).IsTrue();
        _ = await Assert.That(logBytes == HeaderLength()).IsFalse();

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((Committed, Committed, Committed));
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).IsEmpty();
        await AssertOutcomesAsync(reopened);
        _ = await Assert.That(await reopened.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        await AssertCompactedStateAsync(reopened, cancellationToken);
    }

    /// <summary>Recovery retains the acknowledged tail both before and after the prepared WAL becomes active.</summary>
    /// <param name="cut">Zero before publication, one after the data flush, two after the seal flush, three for success, or four for fallback recovery.</param>
    /// <param name="appendTail">Whether to commit another entry after the WAL has been prepared.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0, true)]
    [Arguments(1, true)]
    [Arguments(2, true)]
    [Arguments(3, true)]
    [Arguments(4, true)]
    [Arguments(0, false)]
    [Arguments(2, false)]
    [Arguments(3, false)]
    public async Task PreparedCompactionRecoversTail(int cut, bool appendTail, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-prepared-compaction-recovery");
        var flushToFail = 0;
        var hooks = new IFollowerLogFaultHooksCreateExpectations();
        _ = hooks.Setups.OnFrameWritten();
        _ = hooks.Setups.OnFramesScheduling();
        _ = hooks.Setups.OnCommitAdvanced();
        _ = hooks.Setups.OnBeforeMemoryApply();
        _ = hooks.Setups.OnFlushed().Callback(() => FailIfArmed(flushToFail != 0 && --flushToFail == 0));
        _ = hooks.Setups.OnMetaPublishing();
        _ = hooks.Setups.OnSnapshotPublishing();
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks.Instance() };
        var log = await SeedAsync(dir, options, cancellationToken);
        GroupSnapshotCheckpointStore.PreparedSnapshot? prepared = null;
        try
        {
            var originalPath = GroupLogFile.ActivePath(dir, GroupId);
            await using (log)
            {
                var plan = await CaptureSecondCompactionAsync(log, cancellationToken);
                prepared = await log.PrepareCompactionAsync(plan, cancellationToken);
                _ = await Assert.That(prepared).IsNotNull();
                await AssertUnsealedWalAsync(dir, originalPath, plan, cancellationToken);
                if (appendTail)
                    await AppendAcknowledgedTailAsync(log, cancellationToken);

                if (cut is 1 or 2)
                {
                    // With no tail the first flush during finalization seals the WAL.
                    flushToFail = appendTail ? cut : 1;
                    _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(
                        log.CompactPreparedAsync(plan, prepared!, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
                    _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
                }

                if (cut >= 3)
                    _ = await Assert.That(await log.CompactPreparedAsync(plan, prepared!, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);

                _ = await Assert.That(string.Equals(GroupLogFile.ActivePath(dir, GroupId), originalPath, StringComparison.Ordinal)).IsEqualTo(cut < 2);
            }

            if (cut == 4)
                await GroupSnapshotFile.TearAsync(dir, GroupId, prepared!.Pointer.Slot, 0, cancellationToken);

            await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
            await reopened.OpenAsync(cancellationToken);
            await AssertRecoveredTailAsync(reopened, cut is 0 or 4 ? 3UL : 4UL, appendTail, cancellationToken);
            await AssertOutcomesAsync(reopened);
        }
        finally
        {
            prepared?.Dispose();
        }
    }

    /// <summary>A prepared generation cannot change the snapshot after the log has observed another term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreparedCompactionRejectsChangedTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-prepared-compaction-term");
        await using var log = await SeedAsync(dir, null, cancellationToken);
        var plan = await CaptureSecondCompactionAsync(log, cancellationToken);
        using var prepared = await log.PrepareCompactionAsync(plan, cancellationToken);
        _ = await Assert.That(prepared).IsNotNull();
        _ = await Assert.That(await log.ObserveTermAsync(2UL, cancellationToken)).IsEqualTo(2UL);
        _ = await Assert.That(await log.CompactPreparedAsync(plan, prepared!, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);
        _ = await Assert.That((await RetentionAsync(log, cancellationToken)).SnapshotIndex).IsEqualTo(Committed);
    }

    /// <summary>A range read below the snapshot baseline is not retained, while one starting right above it verifies against the baseline term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadBelowSnapshotBaselineIsNotRetained(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-read");
        await using var log = await SeedAsync(dir, null, cancellationToken);
        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", Committed + 1, 1UL, "after"), cancellationToken);

        var compacted = await log.ReadEntriesAsync(Committed, 1, cancellationToken);
        var aboveBaseline = await log.ReadEntriesAsync(Committed + 1, 1, cancellationToken);

        _ = await Assert.That((compacted.Retained, compacted.PrevLogIndex, compacted.PrevLogTerm)).IsEqualTo((false, Committed - 1, 0UL));
        _ = await Assert.That(compacted.Entries).IsEmpty();
        _ = await Assert.That((aboveBaseline.Retained, aboveBaseline.PrevLogIndex, aboveBaseline.PrevLogTerm, aboveBaseline.LastLogIndex, aboveBaseline.CommitIndex))
                        .IsEqualTo((true, Committed, 1UL, Committed + 1, Committed));
        _ = await Assert.That(aboveBaseline.Entries.Count).IsEqualTo(1);
        _ = await Assert.That((aboveBaseline.Entries[0].LogIndex, Encoding.UTF8.GetString(aboveBaseline.Entries[0].Payload.Span))).IsEqualTo((Committed + 1, "after"));
    }

    /// <summary>A snapshot past the configured maximum size is refused before anything is written, readiness included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesOversizedSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-oversized");
        await using var log = await SeedAsync(dir, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, MaxSnapshotBytes = 64 }, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.SnapshotTooLarge);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>An index that is not both the commit and the applied index is refused before anything changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesUnappliedIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-unapplied");
        await using var log = await SeedAsync(dir, null, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Committed - 1, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>An entry at or below the index with an unresolved outcome refuses the compaction before anything changes, readiness included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesUnresolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-unresolved");
        await using var log = await SeedAsync(dir, null, cancellationToken);
        _ = log.Idempotency.Reserve("client", "in-flight", [9], GroupRecordKind.UserMutation, 2UL, 1UL, GroupReserveKit.Unlogged);

        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.UnresolvedOutcome);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>
    /// A compaction publishes no metadata of its own: after it and a reopen, the last, commit and applied indexes are as before the reopen,
    /// and the next append takes the following index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReopenAfterCompactionKeepsIndexes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-indexes");
        var hooks = new CountingFollowerLogFaultHooks();
        FollowerLogStatus before;
        await using (var log = await SeedAsync(dir, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks }, cancellationToken))
        {
            var writesBefore = hooks.Snapshot();
            _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
            _ = await Assert.That(hooks.Snapshot().Since(in writesBefore).MetaWrites).IsEqualTo(0L);
            before = await log.GetStatusAsync(cancellationToken);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        var after = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((after.LastLogIndex, after.CommitIndex, after.LastAppliedIndex)).IsEqualTo((before.LastLogIndex, before.CommitIndex, before.LastAppliedIndex));
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(Committed);
        var next = await reopened.AppendAsync(FollowerFoundationScenario.Append("leader", Committed + 1, 1UL, "after"), cancellationToken);
        _ = await Assert.That((next.Success, next.LastLogIndex)).IsEqualTo((true, Committed + 1));
    }

    private static async Task<GroupSnapshotCompaction> CaptureSecondCompactionAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        const ulong boundary = Committed + 1UL;
        _ = await Assert.That((await log.AppendAsync(FollowerFoundationScenario.Append("leader", boundary, 1UL, "boundary"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(boundary, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceAppliedAsync(boundary, cancellationToken)).Success).IsTrue();
        var (_, capture) = await log.CaptureCompactionAsync(boundary, cancellationToken);
        var plan = capture.GetValueOrDefault();
        _ = await Assert.That(plan.Snapshot.LastIncludedIndex).IsEqualTo(boundary);
        return plan;
    }

    private static async Task AssertUnsealedWalAsync(string dir, string originalPath, GroupSnapshotCompaction plan, CancellationToken cancellationToken)
    {
        var source = await GroupLogFile.ReadSlotBytesAsync(dir, GroupId, 0, cancellationToken);
        var standby = await GroupLogFile.ReadSlotBytesAsync(dir, GroupId, 1, cancellationToken);
        _ = await Assert.That(GroupLogFile.ActivePath(dir, GroupId)).IsEqualTo(originalPath);
        _ = await Assert.That(standby).IsNotNull();
        _ = await Assert.That(standby.AsSpan(0, GroupLogSlots.HeaderSize).SequenceEqual(new byte[GroupLogSlots.HeaderSize])).IsTrue();
        _ = await Assert.That(standby.AsSpan(GroupLogSlots.HeaderSize).SequenceEqual(source.AsSpan(int.CreateChecked(plan.RetainedOffset)))).IsTrue();
    }

    private static async Task AssertCompactedStateAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((Committed, 1UL, Committed, Committed));
        _ = await Assert.That(await log.GetTermAtAsync(Committed, cancellationToken)).IsEqualTo(1UL);
        _ = await Assert.That(log.SnapshotPath).IsNotNull();
        await AssertOutcomesAsync(log);
    }

    private static async Task AppendAcknowledgedTailAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        _ = await Assert.That((await log.AppendAsync(FollowerFoundationScenario.Append("leader", 5UL, 1UL, "acknowledged tail"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(5UL, cancellationToken)).Success).IsTrue();
    }

    private static async Task AssertRecoveredTailAsync(FollowerLog log, ulong snapshotIndex, bool appendTail, CancellationToken cancellationToken)
    {
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await log.GetStatusAsync(cancellationToken);
        var commit = appendTail ? 5UL : 4UL;
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((commit, commit, 4UL));
        _ = await Assert.That((await RetentionAsync(log, cancellationToken)).SnapshotIndex).IsEqualTo(snapshotIndex);
        var entries = await log.GetCommittedEntriesAsync(cancellationToken);
        _ = await Assert.That(entries.Count).IsEqualTo(appendTail ? 1 : 0);
        if (appendTail)
            _ = await Assert.That(Encoding.UTF8.GetString(entries[0].Payload.Span)).IsEqualTo("acknowledged tail");
    }

    private static async Task AssertOutcomesAsync(FollowerLog log)
    {
        for (byte index = 1; index <= Committed; index++)
        {
            _ = await Assert.That(log.Idempotency.Lookup("client", OperationId(index), [index], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
            await SequenceAssert.EqualAsync([index, index], record.OutcomePayload.ToArray());
        }
    }

    private static async Task AssertUnchangedAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var retention = await RetentionAsync(log, cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(log.SnapshotPath).IsNull();
        _ = await Assert.That((retention.RetainedEntries, retention.SnapshotIndex)).IsEqualTo((EntryCount, 0UL));
    }

    private static void FailIfArmed(bool armed)
    {
        if (armed)
            throw new IOException("simulated crash at a compaction boundary.");
    }

    private static long HeaderLength() => GroupLogSlots.HeaderSize;

    private static string OperationId(byte index) => "op-" + index;

    private static ValueTask<FollowerLogRetention> RetentionAsync(IFollowerLog log, CancellationToken cancellationToken) => log.GetRetentionAsync(cancellationToken);

    /// <summary>Opens a log holding entries 1 to 3, committed and applied, each with its resolved outcome.</summary>
    /// <param name="dir">The node data directory.</param>
    /// <param name="options">The log options, or <see langword="null" /> for the defaults.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The open log.</returns>
    private static async Task<FollowerLog> SeedAsync(string dir, FollowerLogOptions? options, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options ?? new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System });
        try
        {
            await log.OpenSeededAsync(cancellationToken);
            for (byte index = 1; index <= Committed; index++)
            {
                _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", index, 1UL, "value-" + index), cancellationToken);
                _ = log.Idempotency.Reserve("client", OperationId(index), [index], GroupRecordKind.UserMutation, index, 1UL, GroupReserveKit.Unlogged);
                _ = log.Idempotency.TryResolve("client", OperationId(index), new[] { index, index }, index, 1UL);
            }

            _ = await log.AdvanceCommitAsync(Committed, cancellationToken);
            _ = await log.AdvanceAppliedAsync(Committed, cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
