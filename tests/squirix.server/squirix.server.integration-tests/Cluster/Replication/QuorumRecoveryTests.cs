using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Crash-boundary recovery checks for committed and applied group indexes.</summary>
public sealed class QuorumRecoveryTests : NodeIntegrationTestBase
{
    private const string GroupId = "quorum-recovery";

    /// <summary>A committed-not-applied entry is selected once, then its durable applied index suppresses replay.</summary>
    [Fact]
    public async Task CommittedNotAppliedReplaysOnce()
    {
        using var directory = new TempDirectory("squirix-quorum-recovery");
        await using (var log = Open(directory))
        {
            await log.OpenAsync(DefaultCancellationToken);
            _ = await log.AppendAsync(AppendOne(), DefaultCancellationToken);
            _ = await log.AdvanceCommitAsync(1, DefaultCancellationToken);
        }

        await using (var recovered = Open(directory))
        {
            await recovered.OpenAsync(DefaultCancellationToken);
            var status = await recovered.GetStatusAsync(DefaultCancellationToken);
            var committed = await recovered.GetCommittedEntriesAsync(DefaultCancellationToken);
            Assert.Equal(1, CountAfter(committed, status.LastAppliedIndex));
            _ = await recovered.AdvanceAppliedAsync(1, DefaultCancellationToken);
        }

        await using var reopened = Open(directory);
        await reopened.OpenAsync(DefaultCancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(DefaultCancellationToken);
        var reopenedCommitted = await reopened.GetCommittedEntriesAsync(DefaultCancellationToken);
        Assert.Equal(0, CountAfter(reopenedCommitted, reopenedStatus.LastAppliedIndex));
        Assert.Equal(1UL, reopenedStatus.CommitIndex);
        Assert.Equal(1UL, reopenedStatus.LastAppliedIndex);
    }

    private static FollowerLogAppendRequest AppendOne() => new(
        "leader-a",
        1,
        0,
        0,
        0,
        new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(1, 1, Encoding.UTF8.GetBytes("committed"))]));

    private static int CountAfter(IReadOnlyList<FollowerLogEntry> entries, ulong appliedIndex)
    {
        var count = 0;
        for (var index = 0; index < entries.Count; index++)
        {
            if (entries[index].LogIndex > appliedIndex)
                count++;
        }

        return count;
    }

    private static FollowerLog Open(TempDirectory directory) =>
        new(directory, GroupId, GroupComposition.Create(GroupId));
}
