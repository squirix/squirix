using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Recovery derives the last log index from the log file, so an append that changes only the last log index publishes no metadata.</summary>
public sealed class ReplicaLastIndexRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-last-index";

    /// <summary>An append publishes no metadata, also when it announces a commit index; making the announced commit index durable publishes once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendPublishesNoMetadata(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-no-publish");
        var composition = GroupComposition.Create(GroupId);
        var hooks = new CountingFollowerLogFaultHooks();

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks }))
        {
            await log.OpenSeededAsync(cancellationToken);

            // The first append adopts the leader term, which is published; later appends change the last log index only.
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            var before = hooks.Snapshot();
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            var lastIndexOnly = hooks.Snapshot().Since(in before);
            _ = await Assert.That((lastIndexOnly.MetaWrites, lastIndexOnly.FrameWrites)).IsEqualTo((0L, 2L));

            var carrying = new FollowerLogAppendRequest("leader", 1UL, 3UL, 1UL, 3UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(4UL, 1UL, Encoding.UTF8.GetBytes("d"))));
            before = hooks.Snapshot();
            _ = await log.AppendAsync(carrying, cancellationToken);
            var commitAdvancing = hooks.Snapshot().Since(in before);
            _ = await Assert.That((commitAdvancing.MetaWrites, commitAdvancing.FrameWrites)).IsEqualTo((0L, 1L));

            before = hooks.Snapshot();
            await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
            var madeDurable = hooks.Snapshot().Since(in before);
            _ = await Assert.That((madeDurable.MetaWrites, madeDurable.FrameWrites)).IsEqualTo((1L, 0L));
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((4UL, 3UL));
        var committed = await reopened.GetCommittedEntriesAsync(0UL, 10, cancellationToken);
        _ = await Assert.That(IndexesOf(committed)).IsEqualTo("1,2,3");
    }

    /// <summary>A leader that restarts after appends no publish followed uses the walked last index for status, vote freshness, and the next append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderRestartUsesWalkedLastIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-leader-restart");
        var composition = GroupComposition.Create(GroupId);
        var hooks = new CountingFollowerLogFaultHooks();

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks }))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            var before = hooks.Snapshot();
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
            _ = await Assert.That(hooks.Snapshot().Since(in before).MetaWrites).IsEqualTo(0L);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm)).IsEqualTo((3UL, 1UL));

        var behind = await reopened.RequestVoteAsync(new ElectionVoteRequest("candidate", 2UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That((behind.Granted, behind.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.StaleLog));
        var current = await reopened.RequestVoteAsync(new ElectionVoteRequest("candidate", 2UL, 3UL, 1UL), cancellationToken);
        _ = await Assert.That(current.Granted).IsTrue();

        var next = await reopened.AppendAsync(new FollowerLogAppendRequest("candidate", 2UL, 3UL, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(4UL, 2UL, Encoding.UTF8.GetBytes("d")))), cancellationToken);
        _ = await Assert.That((next.Success, next.LastLogIndex)).IsEqualTo((true, 4UL));
    }

    /// <summary>A crash between a snapshot publish and its metadata publish never raises applied past frames the log holds above the boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogAboveBoundaryKeepsEntriesUnapplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-above-boundary");
        var composition = GroupComposition.Create(GroupId);
        await SeedUnpublishedTailAsync(dir, composition, 4UL, cancellationToken);
        await PublishSnapshotAsync(dir, GroupId, 3UL, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((3UL, 0UL, 4UL));
        var pending = await reopened.GetCommittedEntriesAsync(status.LastAppliedIndex, 10, cancellationToken);
        _ = await Assert.That(IndexesOf(pending)).IsEqualTo("1,2,3");
        _ = await Assert.That((await ReadMetaAsync(dir, GroupId, cancellationToken)).LastAppliedIndex).IsEqualTo(0UL);
    }

    /// <summary>A log that ends exactly at the snapshot boundary still holds the entries up to it, so applied is not raised.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogEndingAtBoundaryKeepsEntriesUnapplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-at-boundary");
        var composition = GroupComposition.Create(GroupId);
        await SeedUnpublishedTailAsync(dir, composition, 3UL, cancellationToken);
        await PublishSnapshotAsync(dir, GroupId, 3UL, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((3UL, 0UL, 3UL));
        var pending = await reopened.GetCommittedEntriesAsync(status.LastAppliedIndex, 10, cancellationToken);
        _ = await Assert.That(IndexesOf(pending)).IsEqualTo("1,2,3");
    }

    /// <summary>A log that ends below the snapshot boundary is covered by the snapshot, so applied rises to the boundary and is published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogBelowBoundaryRaisesApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-below-boundary");
        var composition = GroupComposition.Create(GroupId);
        await SeedUnpublishedTailAsync(dir, composition, 2UL, cancellationToken);
        await PublishSnapshotAsync(dir, GroupId, 4UL, cancellationToken);

        await using (var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await reopened.OpenAsync(cancellationToken);
            var status = await reopened.GetStatusAsync(cancellationToken);
            _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((4UL, 4UL, 2UL));
        }

        _ = await Assert.That((await ReadMetaAsync(dir, GroupId, cancellationToken)).LastAppliedIndex).IsEqualTo(4UL);
    }

    /// <summary>A group without a log file is covered by the snapshot, so applied rises to the boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingLogRaisesApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-missing-log");
        await AssertBoundaryRaisedAsync(dir, false, false, cancellationToken);
    }

    /// <summary>An empty log file is covered by the snapshot, so applied rises to the boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyLogRaisesApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-empty-log");
        await AssertBoundaryRaisedAsync(dir, true, false, cancellationToken);
    }

    /// <summary>A header-only log file is covered by the snapshot, so applied rises to the boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeaderOnlyLogRaisesApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-last-index-header-only");
        await AssertBoundaryRaisedAsync(dir, true, true, cancellationToken);
    }

    internal static FollowerLogAppendRequest Append(ulong index, string payload) => FollowerFoundationScenario.Append("leader", index, 1UL, payload);

    internal static string IndexesOf(IReadOnlyList<FollowerLogEntry> entries)
    {
        var indexes = new List<string>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
            indexes.Add(entries[i].LogIndex.ToString(CultureInfo.InvariantCulture));

        return string.Join(',', indexes);
    }

    internal static Task PublishSnapshotAsync(TempDirectory dir, string groupId, ulong included, CancellationToken cancellationToken) =>
        new GroupSnapshotStore(dir, groupId).PublishAsync(
            new GroupSnapshot(groupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, included, included, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch),
            cancellationToken);

    internal static Task<GroupLogMetadata> ReadMetaAsync(TempDirectory dir, string groupId, CancellationToken cancellationToken) =>
        GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, groupId), cancellationToken);

    /// <summary>Leaves a log holding <paramref name="lastIndex" /> frames of which the last metadata publish covered up to commit index two.</summary>
    /// <param name="dir">The group directory.</param>
    /// <param name="composition">The group composition.</param>
    /// <param name="lastIndex">The highest frame index to append, at least two.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the log is closed.</returns>
    private static async Task SeedUnpublishedTailAsync(TempDirectory dir, GroupComposition composition, ulong lastIndex, CancellationToken cancellationToken)
    {
        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        // These appends publish no metadata.
        for (var index = 3UL; index <= lastIndex; index++)
            _ = await log.AppendAsync(Append(index, "tail"), cancellationToken);
    }

    private static void WriteLogFile(TempDirectory dir, bool withHeader)
    {
        var path = GroupStoragePaths.GetLogSlotPath(dir, GroupId, 0);
        File.OpenHandle(path, FileMode.Create, FileAccess.Write).Dispose();
        if (withHeader)
            GroupLogFile.WriteHeader(path, 1UL);
    }

    private static async Task AssertBoundaryRaisedAsync(TempDirectory dir, bool createLog, bool withHeader, CancellationToken cancellationToken)
    {
        var composition = GroupComposition.Create(GroupId);
        await using (var seed = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
            await seed.OpenSeededAsync(cancellationToken);

        // The seed open creates an empty log file; the missing-log shape needs it removed, so recovery meets no file at all.
        if (createLog)
            WriteLogFile(dir, withHeader);
        else
            File.Delete(GroupLogFile.ActivePath(dir, GroupId));

        _ = await Assert.That(File.Exists(GroupLogFile.ActivePath(dir, GroupId))).IsEqualTo(createLog);
        await PublishSnapshotAsync(dir, GroupId, 2UL, cancellationToken);

        await using (var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await reopened.OpenAsync(cancellationToken);
            var status = await reopened.GetStatusAsync(cancellationToken);
            _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((2UL, 2UL, 2UL));
        }

        _ = await Assert.That((await ReadMetaAsync(dir, GroupId, cancellationToken)).LastAppliedIndex).IsEqualTo(2UL);
    }
}
