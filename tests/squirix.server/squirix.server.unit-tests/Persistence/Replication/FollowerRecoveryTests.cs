using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Restart recovery of the replica-group follower log applies only the committed prefix.</summary>
[Immutable]
public sealed class FollowerRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>Corruption inside the committed prefix closes readiness and startup fails.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedPrefixConflictFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-committed-conflict");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        }

        await FollowerLogTestKit.CorruptByteAsync(logPath, 8, cancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>An entry committed durably but not yet applied to memory is replayed after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrashBeforeMemoryApplyReplaysEntry(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-crash-before-apply");
        var crashFaults = new CrashBeforeApplyFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), crashFaults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<IOException, IReadOnlyList<FollowerLogEntry>>(log.GetCommittedEntriesAsync(cancellationToken));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("a");
    }

    /// <summary>A corrupt or torn divergent tail is safely truncated on restart without touching the committed prefix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DivergentTailIsTruncatedOrQuarantined(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-divergent-tail");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AppendAsync(Append(4UL, 1UL, "d"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        }

        await FollowerLogTestKit.CorruptTailAsync(logPath, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("ab");
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(3UL);
    }

    /// <summary>An empty log file with a nonzero committed index fails readiness to restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyLogWithCommittedIndexFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-empty-log");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await File.WriteAllBytesAsync(logPath, [], cancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(ex.Message).Contains("commit index", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A missing log file with a nonzero committed index fails readiness to restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingLogFailsReadinessAtCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-missing-log");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        File.Delete(logPath);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(ex.Message).Contains("commit index", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A missing metadata file with an existing log fails readiness instead of truncating the durable log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingMetaWithExistingLogFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-missing-meta");
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        File.Delete(metaPath);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(ex.Message).Contains("metadata is missing", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>After restart only the committed prefix is exposed; uncommitted entries are not applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartAppliesCommittedPrefixOnly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-committed-prefix");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("ab");
    }

    /// <summary>Pending operations are rebuilt from the uncommitted tail after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRebuildsPendingOperationsFromTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-pending-rebuild");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(2UL);
        _ = await Assert.That(tail[1].LogIndex).IsEqualTo(3UL);
    }

    /// <summary>A failed append that already wrote frames must not leave a stale suffix that a shorter retry and recovery accept.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShorterRetryTruncatesStaleSuffix(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-stale-suffix");
        var faults = new FrameWriteFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults))
        {
            await log.OpenAsync(cancellationToken);

            // The three-entry batch is durably written, but the append faults right after the write, so the
            // in-memory logical end never advances while the physical file already holds all three frames.
            var longBatch = new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                0UL,
                0UL,
                0UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(
                    new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")),
                    new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("b")),
                    new FollowerLogEntry(3UL, 1UL, Encoding.UTF8.GetBytes("c"))));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AppendAsync(longBatch, cancellationToken));
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);

            // The retry is shorter (two entries). Its durable write must size the file to its own end so the
            // stale third frame written by the failed append is truncated and never surfaces after recovery.
            var shortBatch = new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                0UL,
                0UL,
                0UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(
                    new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")),
                    new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("b"))));
            var retry = await log.AppendAsync(shortBatch, cancellationToken);
            _ = await Assert.That(retry.Success).IsTrue();
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("ab");
    }

    /// <summary>A truncate that modifies the file and then faults must not leave the in-memory index ahead of durable storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncateFailureReconcilesMemory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-fault");
        var faults = new TruncateFlushFaults();

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
        _ = await log.AppendAsync(Append(4UL, 1UL, "d"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        // Arm the fault so the durable truncate's flush throws after the file has been sized back down.
        faults.Arm();

        // New leader (term 2) rewrites the uncommitted index 3. The durable truncate applies (file shortened
        // through index 2) but then faults, so the in-memory log must be reconciled, not left ahead.
        var batch = new FollowerLogAppendRequest(
            "leader-2",
            2UL,
            2UL,
            1UL,
            0UL,
            ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("C"))));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AppendAsync(batch, cancellationToken));

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).IsEmpty();

        // A subsequent request must not validate against the vanished suffix; the rewrite now persists.
        var retry = await log.AppendAsync(batch, cancellationToken);
        _ = await Assert.That(retry.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        var entry = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(entry.LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(entry.Term).IsEqualTo(2UL);
    }

    /// <summary>A conflicting uncommitted tail truncated during appending is absent after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatedConflictIsAbsentAfterRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-truncated-conflict");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 2UL, "A"), cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].Term).IsEqualTo(2UL);
        _ = await Assert.That(FollowerLogTestKit.Payload(tail)).IsEqualTo("A");
    }

    /// <summary>Truncating a corrupt divergent tail at a startup releases the pending tail entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatedTailReleasesPendingReservation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-truncate-releases");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await FollowerLogTestKit.CorruptTailAsync(logPath, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(2UL);
    }

    /// <summary>Uncommitted entries are never surfaced as committed, in memory or after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedTailIsNotApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-recovery-uncommitted-tail");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

            _ = await Assert.That(await log.GetCommittedEntriesAsync(cancellationToken)).HasSingleItem();
            _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).HasSingleItem();
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).HasSingleItem();
        _ = await Assert.That(await reopened.GetUncommittedTailAsync(cancellationToken)).HasSingleItem();
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        index == 1UL ? 0UL : term,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))));

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));

    /// <summary>Fault hooks that simulate a crash at the memory-apply boundary exactly once.</summary>
    private sealed class CrashBeforeApplyFaults : IFollowerLogFaultHooks
    {
        private bool _fired;

        public void OnBeforeMemoryApply()
        {
            if (_fired)
                return;

            _fired = true;
            throw new IOException("simulated crash before memory apply.");
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }
    }

    /// <summary>Fault hooks that fault right after a frame write, before the flush, exactly once.</summary>
    private sealed class FrameWriteFaults : IFollowerLogFaultHooks
    {
        private bool _fired;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
            if (_fired)
                return;

            _fired = true;
            throw new IOException("simulated failure after the frame write.");
        }
    }

    /// <summary>Fault hooks that throw from the flush boundary once armed, exactly once.</summary>
    private sealed class TruncateFlushFaults : IFollowerLogFaultHooks
    {
        private bool _armed;
        private bool _fired;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
            if (!_armed || _fired)
                return;

            _fired = true;
            throw new IOException("simulated failure after the durable truncate.");
        }

        public void OnFrameWritten()
        {
        }

        internal void Arm() => _armed = true;
    }
}
