using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Ordered durable append and rejection rules of the replica-group follower log.</summary>
public sealed class FollowerLogTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>Consecutive entries become durably visible after each append is acknowledged.</summary>
    [Fact]
    public async Task AppendsConsecutiveEntryAfterDurableFlush()
    {
        using var dir = new TempDirectory("squirix-follower-log-append");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);

        var first = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        var second = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);

        Assert.True(first.Success);
        Assert.True(second.Success);
        Assert.Equal(2UL, log.GetStatus().LastLogIndex);
        Assert.Equal(2, log.GetUncommittedTail().Count);
    }

    /// <summary>Replaying an identical entry acknowledges idempotently without a second journal effect.</summary>
    [Fact]
    public async Task AcknowledgesIdenticalDuplicateOnce()
    {
        using var dir = new TempDirectory("squirix-follower-log-duplicate");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);

        var first = await log.AppendAsync(Append(1UL, 1UL, "dup"), DefaultCancellationToken);
        var second = await log.AppendAsync(Append(1UL, 1UL, "dup"), DefaultCancellationToken);

        Assert.True(first.Success);
        Assert.True(second.Success);
        Assert.Equal(1UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>A gap in the batch or a conflicting existing entry is rejected without any append.</summary>
    [Fact]
    public async Task RejectsGapAndConflictingEntry()
    {
        using var dir = new TempDirectory("squirix-follower-log-reject");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "one"), DefaultCancellationToken);

        var gap = await log.AppendAsync(Append(3UL, 1UL, "three"), DefaultCancellationToken);
        Assert.False(gap.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, gap.RefusalCode);
        Assert.Equal(1UL, log.GetStatus().LastLogIndex);

        var conflict = await log.AppendAsync(Append(1UL, 2UL, "different"), DefaultCancellationToken);
        Assert.False(conflict.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, conflict.RefusalCode);
        Assert.Equal(1UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>An append carrying a lower term than the durable term is rejected before any mutation.</summary>
    [Fact]
    public async Task RejectsStaleTermBeforeAppend()
    {
        using var dir = new TempDirectory("squirix-follower-log-stale-term");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 5UL, "x"), DefaultCancellationToken);

        var stale = await log.AppendAsync(Append(2UL, 4UL, "y"), DefaultCancellationToken);
        Assert.False(stale.Success);
        Assert.Equal(FollowerLogRefusal.StaleTerm, stale.RefusalCode);
        Assert.Equal(1UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>The commit index never moves backward even when a lower request arrives.</summary>
    [Fact]
    public async Task CommitIndexNeverMovesBackward()
    {
        using var dir = new TempDirectory("squirix-follower-log-commit-backward");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);

        var back = await log.AdvanceCommitAsync(0UL, DefaultCancellationToken);
        Assert.True(back.Success);
        Assert.Equal(1UL, log.GetStatus().CommitIndex);
    }

    /// <summary>A commit index beyond the durable last index is refused.</summary>
    [Fact]
    public async Task DoesNotCommitBeyondDurableLastIndex()
    {
        using var dir = new TempDirectory("squirix-follower-log-commit-beyond");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);

        var result = await log.AdvanceCommitAsync(9UL, DefaultCancellationToken);
        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(0UL, log.GetStatus().CommitIndex);
    }

    /// <summary>A crafted group identifier cannot escape the storage root because segments are hex-encoded.</summary>
    [Fact]
    public void GroupIdCannotEscapeStorageRoot()
    {
        using var dir = new TempDirectory("squirix-follower-log-escape");
        const string evil = "..\\..\\escape";

        var segment = GroupStoragePaths.EncodeGroupSegment(evil);
        Assert.DoesNotContain("..", segment, StringComparison.Ordinal);

        var path = GroupStoragePaths.GetGroupDirectory(dir, evil);
        Assert.StartsWith(dir, path, StringComparison.Ordinal);
    }

    /// <summary>Opening a group outside the local static composition does not create any storage.</summary>
    [Fact]
    public async Task UnknownGroupDoesNotCreateStorageDirectory()
    {
        using var dir = new TempDirectory("squirix-follower-log-unknown-group");
        var composition = GroupComposition.Empty();

        await using var log = new FollowerLog(dir, "unknown", composition);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(log.OpenAsync(DefaultCancellationToken));

        var root = GroupStoragePaths.GetRoot(dir);
        Assert.False(Directory.Exists(root));
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) =>
        new(
            "leader-1",
            term,
            index - 1,
            term,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, System.Text.Encoding.UTF8.GetBytes(payload))]));
}
