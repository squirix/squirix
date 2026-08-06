using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Restart recovery of the replica-group follower log applies only the committed prefix.</summary>
public sealed class FollowerRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

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
        Assert.Equal(2UL, reopened.GetStatus().CommitIndex);
        Assert.Equal("ab", FollowerLogTestKit.Payload(reopened.GetCommittedEntries()));
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

            _ = Assert.Single(log.GetCommittedEntries());
            _ = Assert.Single(log.GetUncommittedTail());
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        _ = Assert.Single(reopened.GetCommittedEntries());
        _ = Assert.Single(reopened.GetUncommittedTail());
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
        Assert.Equal("ab", FollowerLogTestKit.Payload(reopened.GetCommittedEntries()));
        var tail = reopened.GetUncommittedTail();
        _ = Assert.Single(tail);
        Assert.Equal(3UL, tail[0].LogIndex);
    }

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
            _ = NodeExceptionAssert.For<IOException>().Throws(log, static value => _ = value.GetCommittedEntries());
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal("a", FollowerLogTestKit.Payload(reopened.GetCommittedEntries()));
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
        var tail = reopened.GetUncommittedTail();
        Assert.Equal(2, tail.Count);
        Assert.Equal(2UL, tail[0].LogIndex);
        Assert.Equal(3UL, tail[1].LogIndex);
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
        var tail = reopened.GetUncommittedTail();
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].LogIndex);
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
        Assert.Equal(1UL, reopened.GetStatus().LastLogIndex);
        var tail = reopened.GetUncommittedTail();
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].Term);
        Assert.Equal("A", FollowerLogTestKit.Payload(tail));
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

    private static FollowerLog OpenLog(TempDirectory dir) =>
        new(dir, GroupId, GroupComposition.Create(GroupId));

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) =>
        new(
            "leader-1",
            term,
            index - 1,
            index is 1UL ? 0UL : term,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, System.Text.Encoding.UTF8.GetBytes(payload))]));

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
}
