using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Restart recovery of the replica-group follower log applies only the committed prefix.</summary>
[Immutable]
public sealed class FollowerRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>Corruption inside the committed prefix closes readiness and startup fails.</summary>
    [Fact]
    public async Task CommittedPrefixConflictFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-committed-conflict");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        }

        await FollowerLogTestKit.CorruptByteAsync(logPath, 8, DefaultCancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("corrupt", ex.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>An entry committed durably but not yet applied to memory is replayed after restart.</summary>
    [Fact]
    public async Task CrashBeforeMemoryApplyReplaysEntry()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-crash-before-apply");
        var crashFaults = new CrashBeforeApplyFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), crashFaults))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<IOException, IReadOnlyList<FollowerLogEntry>>(log.GetCommittedEntriesAsync(DefaultCancellationToken));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal("a", FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(DefaultCancellationToken)));
    }

    /// <summary>A corrupt or torn divergent tail is safely truncated on restart without touching the committed prefix.</summary>
    [Fact]
    public async Task DivergentTailIsTruncatedOrQuarantined()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-divergent-tail");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(4UL, 1UL, "d"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        }

        await FollowerLogTestKit.CorruptTailAsync(logPath, DefaultCancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal("ab", FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(DefaultCancellationToken)));
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        _ = Assert.Single(tail);
        Assert.Equal(3UL, tail[0].LogIndex);
    }

    /// <summary>An empty log file with a nonzero committed index fails readiness to restart.</summary>
    [Fact]
    public async Task EmptyLogWithCommittedIndexFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-empty-log");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        await File.WriteAllBytesAsync(logPath, [], TestContext.Current.CancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("commit index", ex.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>A missing log file with a nonzero committed index fails readiness to restart.</summary>
    [Fact]
    public async Task MissingLogWithCommittedIndexFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-missing-log");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        File.Delete(logPath);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("commit index", ex.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>A missing metadata file with an existing log fails readiness instead of truncating the durable log.</summary>
    [Fact]
    public async Task MissingMetaWithExistingLogFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-missing-meta");
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        File.Delete(metaPath);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(DefaultCancellationToken));
        Assert.Contains("metadata is missing", ex.Message, StringComparison.Ordinal);
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    /// <summary>After restart only the committed prefix is exposed; uncommitted entries are not applied.</summary>
    [Fact]
    public async Task RestartAppliesCommittedPrefixOnly()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-committed-prefix");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(2UL, (await reopened.GetStatusAsync(DefaultCancellationToken)).CommitIndex);
        Assert.Equal("ab", FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(DefaultCancellationToken)));
    }

    /// <summary>Pending operations are rebuilt from the uncommitted tail after restart.</summary>
    [Fact]
    public async Task RestartRebuildsPendingOperationsFromTail()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-pending-rebuild");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        Assert.Equal(2, tail.Count);
        Assert.Equal(2UL, tail[0].LogIndex);
        Assert.Equal(3UL, tail[1].LogIndex);
    }

    /// <summary>A failed append that already wrote frames must not leave a stale suffix that a shorter retry and recovery accept.</summary>
    [Fact]
    public async Task ShorterRetryAfterFailedAppendTruncatesStaleSuffix()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-stale-suffix");
        var faults = new FrameWriteFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults))
        {
            await log.OpenAsync(DefaultCancellationToken);

            // The three-entry batch is durably written, but the append faults right after the write, so the
            // in-memory logical end never advances while the physical file already holds all three frames.
            var longBatch = new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                0UL,
                0UL,
                0UL,
                new ReadOnlyMemory<FollowerLogEntry>(
                [
                    new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")),
                    new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("b")),
                    new FollowerLogEntry(3UL, 1UL, Encoding.UTF8.GetBytes("c")),
                ]));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AppendAsync(longBatch, DefaultCancellationToken));
            Assert.Equal(0UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);

            // The retry is shorter (two entries). Its durable write must size the file to its own end so the
            // stale third frame written by the failed append is truncated and never surfaces after recovery.
            var shortBatch = new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                0UL,
                0UL,
                0UL,
                new ReadOnlyMemory<FollowerLogEntry>(
                [
                    new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a")),
                    new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("b")),
                ]));
            var retry = await log.AppendAsync(shortBatch, DefaultCancellationToken);
            Assert.True(retry.Success);
            _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(2UL, (await reopened.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        Assert.Equal("ab", FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(DefaultCancellationToken)));
    }

    /// <summary>A truncate that modifies the file and then faults must not leave the in-memory index ahead of durable storage.</summary>
    [Fact]
    public async Task TruncateFailureReconcilesMemoryAheadOfDurable()
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-fault");
        var faults = new TruncateFlushFaults();

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(4UL, 1UL, "d"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

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
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("C"))]));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AppendAsync(batch, DefaultCancellationToken));

        Assert.Equal(2UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        Assert.Empty(await log.GetUncommittedTailAsync(DefaultCancellationToken));

        // A subsequent request must not validate against the vanished suffix; the rewrite now persists.
        var retry = await log.AppendAsync(batch, DefaultCancellationToken);
        Assert.True(retry.Success);
        Assert.Equal(3UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        var tail = await log.GetUncommittedTailAsync(DefaultCancellationToken);
        var entry = Assert.Single(tail);
        Assert.Equal(3UL, entry.LogIndex);
        Assert.Equal(2UL, entry.Term);
    }

    /// <summary>A conflicting uncommitted tail truncated during appending is absent after restart.</summary>
    [Fact]
    public async Task TruncatedConflictIsAbsentAfterRestart()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-truncated-conflict");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 2UL, "A"), DefaultCancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(1UL, (await reopened.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].Term);
        Assert.Equal("A", FollowerLogTestKit.Payload(tail));
    }

    /// <summary>Truncating a corrupt divergent tail at a startup releases the pending tail entries.</summary>
    [Fact]
    public async Task TruncatedTailReleasesPendingReservation()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-truncate-releases");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(3UL, 1UL, "c"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        }

        await FollowerLogTestKit.CorruptTailAsync(logPath, DefaultCancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        var tail = await reopened.GetUncommittedTailAsync(DefaultCancellationToken);
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].LogIndex);
    }

    /// <summary>Uncommitted entries are never surfaced as committed, in memory or after restart.</summary>
    [Fact]
    public async Task UncommittedTailIsNotApplied()
    {
        using var dir = new TempDirectory("squirix-follower-recovery-uncommitted-tail");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);

            _ = Assert.Single(await log.GetCommittedEntriesAsync(DefaultCancellationToken));
            _ = Assert.Single(await log.GetUncommittedTailAsync(DefaultCancellationToken));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        _ = Assert.Single(await reopened.GetCommittedEntriesAsync(DefaultCancellationToken));
        _ = Assert.Single(await reopened.GetUncommittedTailAsync(DefaultCancellationToken));
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        index == 1UL ? 0UL : term,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))]));

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
