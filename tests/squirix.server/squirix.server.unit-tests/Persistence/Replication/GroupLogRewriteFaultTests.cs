using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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

/// <summary>The group log is rewritten into the inactive of its two generation files; a crash at any point leaves one complete log.</summary>
[Immutable]
public sealed class GroupLogRewriteFaultTests : ServerUnitTestBase
{
    private const string GroupId = "grp-rewrite";

    /// <summary>A compaction that fails before the header is sealed reopens on the old file; one that fails after it reopens on the new file.</summary>
    /// <param name="failedFlush">The log flush of the rewrite that fails.</param>
    /// <param name="activeSlot">The slot the reopened log runs on.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, 0)]
    [Arguments(2, 1)]
    public async Task CompactionFaultReopensOnCommittedFile(int failedFlush, int activeSlot, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-fault");
        var faults = new StateInstallFaultHooks();
        await using (var log = NewLog(dir, faults))
        {
            await SeedAsync(log, cancellationToken);
            faults.FailFlush(failedFlush);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(3UL, cancellationToken));
        }

        await using var reopened = NewLog(dir, null);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, tail.Count)).IsEqualTo((5UL, 3UL, 2));
        _ = await Assert.That(GroupLogFile.ActivePath(dir, GroupId)).IsEqualTo(GroupStoragePaths.GetLogSlotPath(dir, GroupId, activeSlot));
        var other = await GroupLogFile.ReadSlotBytesAsync(dir, GroupId, 1 - activeSlot, cancellationToken);
        _ = await Assert.That(other).IsNotNull();
        _ = await Assert.That(other!.Length).IsEqualTo(0);
    }

    /// <summary>A compaction whose header flush fails leaves the running log failed, so no append is accepted until a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeaderFlushFaultFailsLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-header");
        var faults = new StateInstallFaultHooks();
        await using var log = NewLog(dir, faults);
        await SeedAsync(log, cancellationToken);
        faults.FailFlush(1);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(3UL, cancellationToken));
        var next = await log.AppendAsync(Append(6UL), cancellationToken);

        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That(next.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
    }

    /// <summary>Consecutive compactions alternate between the two files and leave the superseded one empty, with and without a restart between them.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionsAlternateBetweenFiles(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-alternate");
        await using (var log = NewLog(dir, null))
        {
            await SeedAsync(log, cancellationToken);
            _ = await Assert.That(await log.CompactThroughAsync(3UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
            await AssertActiveAsync(dir, 1, cancellationToken);
        }

        await using (var log = NewLog(dir, null))
        {
            await log.OpenAsync(cancellationToken);
            await AssertActiveAsync(dir, 1, cancellationToken);
            _ = await log.AppendAsync(Append(6UL), cancellationToken);
            _ = await log.AdvanceCommitAsync(5UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(5UL, cancellationToken);
            _ = await Assert.That(await log.CompactThroughAsync(5UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
            await AssertActiveAsync(dir, 0, cancellationToken);
        }

        await using var last = NewLog(dir, null);
        await last.OpenAsync(cancellationToken);
        var status = await last.GetStatusAsync(cancellationToken);
        _ = await Assert.That((last.Readiness, status.LastLogIndex, status.CommitIndex)).IsEqualTo((FollowerLogReadiness.Ready, 6UL, 5UL));
        await AssertActiveAsync(dir, 0, cancellationToken);
    }

    /// <summary>A first file cut inside its header is recreated as generation 1 when the metadata exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutFirstFileIsRecreated(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-cut");
        await using (var log = NewLog(dir, null))
            await log.OpenSeededAsync(cancellationToken);

        await CutAsync(GroupStoragePaths.GetLogSlotPath(dir, GroupId, 0), 3, cancellationToken);

        await using var reopened = NewLog(dir, null);
        await reopened.OpenAsync(cancellationToken);

        var bytes = await GroupLogFile.ReadSlotBytesAsync(dir, GroupId, 0, cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(bytes).IsNotNull();
        _ = await Assert.That(GroupLogSlots.Select(bytes!, bytes!.Length, [], -1L, "dir")).IsEqualTo((0, 1UL));
    }

    /// <summary>A damaged header of the only file with committed frames behind it is refused, not reset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DamagedHeaderWithFramesIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-damaged");
        await using (var log = NewLog(dir, null))
            await SeedAsync(log, cancellationToken);

        var path = GroupStoragePaths.GetLogSlotPath(dir, GroupId, 0);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);
        before[0] ^= 0xFF;
        await File.WriteAllBytesAsync(path, before, cancellationToken);

        await using var reopened = NewLog(dir, null);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A log file of an earlier build is refused and left unchanged, and no generation file is created beside it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierBuildLogIsRefusedUnchanged(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-legacy");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var legacy = GroupStoragePaths.GetLegacyLogPath(dir, GroupId);
        byte[] bytes = [1, 2, 3, 4, 5, 6];
        await File.WriteAllBytesAsync(legacy, bytes, cancellationToken);

        await using var log = NewLog(dir, null);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("earlier build", StringComparison.Ordinal);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(legacy, cancellationToken);
        await SequenceAssert.EqualAsync(bytes, after);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetLogSlotPath(dir, GroupId, 0)) || File.Exists(GroupStoragePaths.GetLogSlotPath(dir, GroupId, 1))).IsFalse();
    }

    /// <summary>A state install that fails after the new file is sealed reopens incomplete on the header-only new file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallFaultAfterSealReopensOnNewFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-install");
        using var sourceDir = new TempDirectory("squirix-log-rewrite-install-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
            faults.FailFlush(2);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, StateInstallLeaders.Leader(1UL), cancellationToken));
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((reopened.Readiness, reopened.IsComplete, status.LastLogIndex)).IsEqualTo((FollowerLogReadiness.Ready, false, StateInstallScenario.SnapshotIndex));
        _ = await Assert.That(GroupLogFile.ActivePath(dir, StateInstallScenario.GroupId)).IsEqualTo(GroupStoragePaths.GetLogSlotPath(dir, StateInstallScenario.GroupId, 1));
        var active = await GroupLogFile.ReadSlotBytesAsync(dir, StateInstallScenario.GroupId, 1, cancellationToken);
        _ = await Assert.That(active!.Length).IsEqualTo(GroupLogSlots.HeaderSize);
    }

    /// <summary>After a compaction the appends and the reads use the new file without a restart, and a restart keeps the appended frames.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadsAndAppendsFollowTheNewFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-rewrite-follow");
        await using (var log = NewLog(dir, null))
        {
            await SeedAsync(log, cancellationToken);
            _ = await Assert.That(await log.CompactThroughAsync(3UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
            _ = await log.AppendAsync(Append(6UL), cancellationToken);
            _ = await log.AdvanceCommitAsync(6UL, cancellationToken);

            var read = await log.ReadEntriesAsync(4UL, 10, cancellationToken);
            var visited = new List<ulong>();
            var recent = await log.ReadRecentCommittedAsync(
                10,
                entry =>
                {
                    visited.Add(entry.LogIndex);
                    return true;
                },
                cancellationToken);

            _ = await Assert.That((read.Retained, read.Entries.Count, read.Entries[^1].LogIndex)).IsEqualTo((true, 3, 6UL));
            _ = await Assert.That((recent, visited[0])).IsEqualTo((3, 6UL));
        }

        await using var reopened = NewLog(dir, null);
        await reopened.OpenAsync(cancellationToken);
        var again = await reopened.ReadEntriesAsync(4UL, 10, cancellationToken);
        _ = await Assert.That((again.Entries.Count, again.LastLogIndex)).IsEqualTo((3, 6UL));
    }

    private static FollowerLogAppendRequest Append(ulong index) => FollowerFoundationScenario.Append("leader", index, 1UL, "p");

    private static async Task AssertActiveAsync(TempDirectory dir, int slot, CancellationToken cancellationToken)
    {
        _ = await Assert.That(GroupLogFile.ActivePath(dir, GroupId)).IsEqualTo(GroupStoragePaths.GetLogSlotPath(dir, GroupId, slot));
        var superseded = await GroupLogFile.ReadSlotBytesAsync(dir, GroupId, 1 - slot, cancellationToken);
        _ = await Assert.That(superseded).IsNotNull();
        _ = await Assert.That(superseded!.Length).IsEqualTo(0);
    }

    private static async Task CutAsync(string path, int length, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        await File.WriteAllBytesAsync(path, bytes.AsMemory(0, length), cancellationToken);
    }

    private static FollowerLog NewLog(TempDirectory dir, IFollowerLogFaultHooks? faults) => faults == null
        ? new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System)
        : new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults, TimeProvider.System);

    private static async Task SeedAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        await log.OpenSeededAsync(cancellationToken);
        for (var index = 1UL; index <= 5UL; index++)
            _ = await log.AppendAsync(Append(index), cancellationToken);

        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
    }
}
