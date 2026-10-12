using System;
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

/// <summary>Recovery of the applied watermark against a newer snapshot boundary when the log holds frames beyond what the last metadata publish covered.</summary>
public sealed class ReplicaBoundaryRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-boundary";

    /// <summary>A compacted log that ends below a newer snapshot boundary is covered by the snapshot, so applied rises to the boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedLogBelowBoundaryRaisesApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-boundary-compacted-below");
        var composition = GroupComposition.Create(GroupId);
        await SeedCompactedLogAsync(dir, composition, cancellationToken);
        await ReplicaLastIndexRecoveryTests.PublishSnapshotAsync(dir, GroupId, 9UL, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((9UL, 9UL, 7UL));
    }

    /// <summary>A compacted log that reaches a newer snapshot boundary keeps applied where it is, and the entries above it are pending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedLogPastBoundaryKeepsApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-boundary-compacted-past");
        var composition = GroupComposition.Create(GroupId);
        await SeedCompactedLogAsync(dir, composition, cancellationToken);
        await ReplicaLastIndexRecoveryTests.PublishSnapshotAsync(dir, GroupId, 6UL, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex, status.LastLogIndex)).IsEqualTo((6UL, 2UL, 7UL));
        var pending = await reopened.GetCommittedEntriesAsync(status.LastAppliedIndex, 10, cancellationToken);
        _ = await Assert.That(ReplicaLastIndexRecoveryTests.IndexesOf(pending)).IsEqualTo("3,4,5,6");
    }

    /// <summary>Truncating a conflicting tail and appending in the already observed term publishes nothing, and a restart recovers the post-truncation log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncationWithoutPublishSurvivesRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-boundary-truncate-no-publish");
        var composition = GroupComposition.Create(GroupId);
        var hooks = new CountingFollowerLogFaultHooks();

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks }))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(3UL, "c"), cancellationToken);

            // An empty append of the new leader term makes the log adopt the term, which is the only publish of the new term.
            var adopt = await log.AppendAsync(new FollowerLogAppendRequest("leader-2", 2UL, 3UL, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
            _ = await Assert.That(adopt.Success).IsTrue();

            var before = hooks.Snapshot();
            var conflict = new FollowerLogAppendRequest("leader-2", 2UL, 1UL, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("B"))));
            _ = await Assert.That((await log.AppendAsync(conflict, cancellationToken)).Success).IsTrue();
            _ = await Assert.That(hooks.Snapshot().Since(in before).MetaWrites).IsEqualTo(0L);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm)).IsEqualTo((2UL, 2UL));
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(ReplicaLastIndexRecoveryTests.IndexesOf(tail)).IsEqualTo("1,2");
        _ = await Assert.That(FollowerLogTestKit.Payload(tail)).IsEqualTo("aB");
    }

    /// <summary>A fault at the publish of the applied raise fails readiness, and a fresh open without the fault raises applied and is ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedRaiseIsRetriedOnNextOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-boundary-raise-fault");
        var composition = GroupComposition.Create(GroupId);
        await using (var seed = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await seed.OpenAsync(cancellationToken);
            _ = await seed.AppendAsync(ReplicaLastIndexRecoveryTests.Append(1UL, "a"), cancellationToken);
            _ = await seed.AppendAsync(ReplicaLastIndexRecoveryTests.Append(2UL, "b"), cancellationToken);
        }

        // The metadata already carries the boundary's commit index and term, so the raise is the only publish of the open.
        var boundaryMeta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, string.Empty, 4UL, 0UL, GroupMemberState.Complete, false);
        await WriteMetadataAsync(dir, in boundaryMeta, cancellationToken);
        await ReplicaLastIndexRecoveryTests.PublishSnapshotAsync(dir, GroupId, 4UL, cancellationToken);

        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnMetaPublishing().Callback(static () => throw new IOException("simulated failure at the metadata publish."));
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnBeforeMemoryApply();

        await using (var failing = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = faults.Instance() }))
        {
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(failing.OpenAsync(cancellationToken));
            _ = await Assert.That(failing.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        _ = await Assert.That((await ReplicaLastIndexRecoveryTests.ReadMetaAsync(dir, GroupId, cancellationToken)).LastAppliedIndex).IsEqualTo(0UL);

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((4UL, 4UL));
    }

    private static Task WriteMetadataAsync(TempDirectory dir, in GroupLogMetadata metadata, CancellationToken cancellationToken) =>
        GroupMetadataFile.WriteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), in metadata, cancellationToken);

    /// <summary>Leaves a log compacted through index two, holding frames three to seven, of which the last metadata publish covered up to five.</summary>
    /// <param name="dir">The group directory.</param>
    /// <param name="composition">The group composition.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the log is closed.</returns>
    private static async Task SeedCompactedLogAsync(TempDirectory dir, GroupComposition composition, CancellationToken cancellationToken)
    {
        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenAsync(cancellationToken);
        for (var index = 1UL; index <= 5UL; index++)
            _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(index, "e" + index), cancellationToken);

        _ = await log.AdvanceCommitAsync(5UL, cancellationToken);
        _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);

        // These appends publish no metadata.
        _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(6UL, "e6"), cancellationToken);
        _ = await log.AppendAsync(ReplicaLastIndexRecoveryTests.Append(7UL, "e7"), cancellationToken);
    }
}
