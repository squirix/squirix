using System;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence.Replication;

/// <summary>Ordering rules of the replication append protocol over the durable follower log.</summary>
public sealed class FollowerProtocolOrderingTests
{
    private const string GroupId = "grp-1";

    /// <summary>An out-of-order batch is rejected without any partial append.</summary>
    [Fact]
    public async Task OutOfOrderBatchIsRejectedWithoutPartialAppend()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-gap");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create([GroupId]));
        await log.OpenAsync(TestContext.Current.CancellationToken);

        var result = await log.AppendAsync(Batch([Entry(1UL, 1UL, "a"), Entry(3UL, 1UL, "c")], 0UL), TestContext.Current.CancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, result.RefusalCode);
        Assert.Equal(0UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>A duplicate batch produces exactly one journal effect.</summary>
    [Fact]
    public async Task DuplicateBatchProducesOneJournalEffect()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-duplicate");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create([GroupId]));
        await log.OpenAsync(TestContext.Current.CancellationToken);

        var batch = Batch([Entry(1UL, 1UL, "a"), Entry(2UL, 1UL, "b")], 0UL);
        var first = await log.AppendAsync(batch, TestContext.Current.CancellationToken);
        var second = await log.AppendAsync(batch, TestContext.Current.CancellationToken);

        Assert.True(first.Success);
        Assert.True(second.Success);
        Assert.Equal(2UL, log.GetStatus().LastLogIndex);
    }

    /// <summary>A higher term is persisted durably before the append is acknowledged.</summary>
    [Fact]
    public async Task HigherTermPersistsBeforeResponse()
    {
        using var dir = new TempDirectory("squirix-follower-ordering-higher-term");

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create([GroupId])))
        {
            await log.OpenAsync(TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), TestContext.Current.CancellationToken);

            var higher = new FollowerLogAppendRequest(
                "leader-1",
                9UL,
                1UL,
                1UL,
                0UL,
                new ReadOnlyMemory<FollowerLogEntry>([Entry(2UL, 9UL, "b")]));
            var result = await log.AppendAsync(higher, TestContext.Current.CancellationToken);

            Assert.True(result.Success);
            Assert.Equal(9UL, log.GetStatus().CurrentTerm);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create([GroupId]));
        await reopened.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(9UL, reopened.GetStatus().CurrentTerm);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) =>
        Batch([Entry(index, term, payload)], index - 1);

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) =>
        new(index, term, System.Text.Encoding.UTF8.GetBytes(payload));

    private static FollowerLogAppendRequest Batch(FollowerLogEntry[] entries, ulong prevIndex) =>
        new(
            "leader-1",
            entries.Length > 0 ? entries[0].Term : 1UL,
            prevIndex,
            entries.Length > 0 ? entries[0].Term : 1UL,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>(entries));
}
