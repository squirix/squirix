using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Crash-boundary recovery checks for committed and applied group indexes.</summary>
public sealed class QuorumRecoveryTests : NodeIntegrationTestBase
{
    private const string GroupId = "quorum-recovery";

    /// <summary>A committed-not-applied entry is selected once, then its durable applied index suppresses replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedNotAppliedReplaysOnce(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("squirix-quorum-recovery");
        await using (var log = Open(directory))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(AppendOne(), cancellationToken);
            _ = await log.AdvanceCommitAsync(1, cancellationToken);
        }

        await using (var recovered = Open(directory))
        {
            await recovered.OpenAsync(cancellationToken);
            var status = await recovered.GetStatusAsync(cancellationToken);
            var committed = await recovered.GetCommittedEntriesAsync(cancellationToken);
            _ = await Assert.That(CountAfter(committed, status.LastAppliedIndex)).IsEqualTo(1);
            _ = await recovered.AdvanceAppliedAsync(1, cancellationToken);
        }

        await using var reopened = Open(directory);
        await reopened.OpenAsync(cancellationToken);
        var reopenedStatus = await reopened.GetStatusAsync(cancellationToken);
        var reopenedCommitted = await reopened.GetCommittedEntriesAsync(cancellationToken);
        _ = await Assert.That(CountAfter(reopenedCommitted, reopenedStatus.LastAppliedIndex)).IsEqualTo(0);
        _ = await Assert.That(reopenedStatus.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(reopenedStatus.LastAppliedIndex).IsEqualTo(1UL);
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

    private static FollowerLog Open(TempDirectory directory) => new(directory, GroupId, GroupComposition.Create(GroupId));
}
