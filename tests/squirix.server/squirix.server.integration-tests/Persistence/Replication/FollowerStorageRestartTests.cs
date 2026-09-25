using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Persistence.Replication;

/// <summary>Durability of the replica-group follower log across a process restarts.</summary>
[Immutable]
public sealed class FollowerStorageRestartTests : NodeIntegrationTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>A committed entry survives a process restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedEntrySurvivesProcessRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-restart-committed");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("a");
    }

    /// <summary>Corruption in the committed prefix fails readiness on restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedPrefixCorruptionFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-restart-corruption");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await FollowerLogTestKit.CorruptByteAsync(logPath, 8, cancellationToken);

        await using var reopened = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(ex.Message).Contains("committed log frame", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A crash during commit advance recovers deterministically to the advanced commit index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrashMidCommitAdvanceRecoversCleanly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-restart-crash-commit");
        var crashFaults = new CommitAdvanceFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), crashFaults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AdvanceCommitAsync(1UL, cancellationToken));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("a");
    }

    /// <summary>An uncommitted entry remains invisible to committed reads after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedEntryInvisibleAfterRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-restart-uncommitted");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).HasSingleItem();
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        index == 1UL ? 0UL : term,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))));

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));

    /// <summary>Fault hooks that crash at the commit-advance boundary exactly once.</summary>
    private sealed class CommitAdvanceFaults : IFollowerLogFaultHooks
    {
        private bool _fired;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
            if (_fired)
                return;

            _fired = true;
            throw new IOException("simulated crash during commit advance.");
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }

        public void OnMetaWritten()
        {
        }
    }
}
