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

    /// <summary>A gap in the batch is rejected without any append.</summary>
    [Fact]
    public async Task RejectsGapWithoutAppend()
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
    }

    /// <summary>An uncommitted entry conflicting with the leader's batch is truncated and rewritten.</summary>
    [Fact]
    public async Task TruncatesConflictingUncommittedTail()
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-conflict");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "old"), DefaultCancellationToken);

        var result = await log.AppendAsync(Append(1UL, 2UL, "new"), DefaultCancellationToken);

        Assert.True(result.Success);
        Assert.Equal(1UL, log.GetStatus().LastLogIndex);
        var tail = log.GetUncommittedTail();
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].Term);
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(tail[0].Payload.Span));
    }

    /// <summary>A conflict in the middle of the log truncates the divergent tail and rewrites it with the leader's entries.</summary>
    [Fact]
    public async Task TruncatesMidLogConflictAndRewritesTail()
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-mid");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);

        // New leader (term 2) rewrites index 2 (which conflicts) and appends index 3.
        var batch = new FollowerLogAppendRequest(
            "leader-2",
            2UL,
            1UL,
            1UL,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(2UL, 2UL, System.Text.Encoding.UTF8.GetBytes("B")), new FollowerLogEntry(3UL, 2UL, System.Text.Encoding.UTF8.GetBytes("C"))]));
        var result = await log.AppendAsync(batch, DefaultCancellationToken);

        Assert.True(result.Success);
        Assert.Equal(3UL, log.GetStatus().LastLogIndex);
        var tail = log.GetUncommittedTail();
        Assert.Equal(2, tail.Count);
        Assert.Equal(2UL, tail[0].LogIndex);
        Assert.Equal(2UL, tail[0].Term);
        Assert.Equal("B", System.Text.Encoding.UTF8.GetString(tail[0].Payload.Span));
        Assert.Equal(3UL, tail[1].LogIndex);
        Assert.Equal(2UL, tail[1].Term);
        Assert.Equal("C", System.Text.Encoding.UTF8.GetString(tail[1].Payload.Span));
    }

    /// <summary>A conflict at or below the committed index fails readiness without truncating anything.</summary>
    [Fact]
    public async Task CommittedConflictFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-log-committed-conflict");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        var result = await log.AppendAsync(Append(1UL, 2UL, "x"), DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);
        Assert.Equal(2UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>The log takes ownership of appended payloads; mutating the caller buffer does not change stored entries.</summary>
    [Fact]
    public async Task AppendCopiesPayloadIntoOwnedBuffer()
    {
        using var dir = new TempDirectory("squirix-follower-log-owned-payload");
        var composition = GroupComposition.Create([GroupId]);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);

        var payload = System.Text.Encoding.UTF8.GetBytes("abcd");
        var request = new FollowerLogAppendRequest(
            "leader-1",
            1UL,
            0UL,
            1UL,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(1UL, 1UL, payload)]));
        _ = await log.AppendAsync(request, DefaultCancellationToken);

        payload[0] = 0xFF;
        payload[3] = 0xFF;

        var tail = log.GetUncommittedTail();
        Assert.Equal("abcd", System.Text.Encoding.UTF8.GetString(tail[0].Payload.Span));
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
