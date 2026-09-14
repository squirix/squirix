using System;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence.Replication;

/// <summary>Ordering rules of the replication append protocol over the durable follower log.</summary>
[Immutable]
public sealed class FollowerProtocolOrderingTests : NodeIntegrationTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>A divergent uncommitted tail is truncated and rewritten by the new leader.</summary>
    [Fact]
    public async Task ConflictingTailIsTruncatedAndRewritten()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-conflict");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(DefaultCancellationToken);

        // Old leader (term 1) appends an entry at index 1, then crashes before a majority.
        var first = await log.AppendAsync(Append(1UL, 1UL, "x"), DefaultCancellationToken);
        Assert.True(first.Success);

        // New leader (term 2) rewrites index 1 with a conflicting entry.
        var result = await log.AppendAsync(Append(1UL, 2UL, "y"), DefaultCancellationToken);

        Assert.True(result.Success);
        Assert.Equal(1UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        var tail = await log.GetUncommittedTailAsync(DefaultCancellationToken);
        _ = Assert.Single(tail);
        Assert.Equal(2UL, tail[0].Term);
        Assert.Equal("y", Encoding.UTF8.GetString(tail[0].Payload.Span));
    }

    /// <summary>A duplicate batch produces exactly one journal effect.</summary>
    [Fact]
    public async Task DuplicateBatchProducesOneJournalEffect()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-duplicate");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(DefaultCancellationToken);

        var batch = Batch([Entry(1UL, 1UL, "a"), Entry(2UL, 1UL, "b")], 0UL, 0UL, 1UL);
        var first = await log.AppendAsync(batch, DefaultCancellationToken);
        var logLength = FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId));
        var second = await log.AppendAsync(batch, DefaultCancellationToken);

        Assert.True(first.Success);
        Assert.True(second.Success);
        Assert.Equal(2UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        Assert.Equal(logLength, FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId)));
    }

    /// <summary>A higher term is persisted durably before the appending is acknowledged.</summary>
    [Fact]
    public async Task HigherTermPersistsBeforeResponse()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-higher-term");

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);

            var higher = new FollowerLogAppendRequest("leader-1", 9UL, 1UL, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([Entry(2UL, 9UL, "b")]));
            var result = await log.AppendAsync(higher, DefaultCancellationToken);

            Assert.True(result.Success);
            Assert.Equal(9UL, (await log.GetStatusAsync(DefaultCancellationToken)).CurrentTerm);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(DefaultCancellationToken);
        Assert.Equal(9UL, (await reopened.GetStatusAsync(DefaultCancellationToken)).CurrentTerm);
    }

    /// <summary>An out-of-order batch is rejected without any partial appending.</summary>
    [Fact]
    public async Task OutOfOrderBatchRejectedAtomically()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-gap");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(DefaultCancellationToken);

        var result = await log.AppendAsync(Batch([Entry(1UL, 1UL, "a"), Entry(3UL, 1UL, "c")], 0UL, 0UL, 1UL), DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, result.RefusalCode);
        Assert.Equal(0UL, (await log.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => Batch([Entry(index, term, payload)], index - 1, index == 1UL ? 0UL : term, term);

    private static FollowerLogAppendRequest Batch(FollowerLogEntry[] entries, ulong prevIndex, ulong prevTerm, ulong term) => new(
        "leader-1",
        term,
        prevIndex,
        prevTerm,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>(entries));

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));
}
