using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Persistence.Replication.StateInstallLeaders;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A follower log that fails at one of the two slot writes of a snapshot publish reopens to a consistent snapshot and log.</summary>
[Immutable]
public sealed class FollowerLogSnapshotSlotFaultTests : ServerUnitTestBase
{
    private const string GroupId = StateInstallScenario.GroupId;

    /// <summary>
    /// A compaction that fails at its first snapshot slot write keeps the previous snapshot and the whole log; one that fails at its second
    /// slot write has published the new snapshot, and the log, which is rewritten afterwards, is still whole.
    /// </summary>
    /// <param name="ordinal">The snapshot slot write that fails.</param>
    /// <param name="expectedSnapshotIndex">The last included index of the snapshot a reopen finds.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, 2UL)]
    [Arguments(2, 4UL)]
    public async Task CompactionSlotFaultReopensConsistent(int ordinal, ulong expectedSnapshotIndex, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-slot-fault");
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 4UL, 1UL, "d"), cancellationToken);
            _ = await log.AdvanceCommitAsync(4UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(4UL, cancellationToken);
            faults.FailSnapshotSlotWrite(ordinal);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(4UL, cancellationToken));
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).ReadPublishedAsync(cancellationToken)).IsNotNull();

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(expectedSnapshotIndex);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((4UL, 4UL, 4UL));
    }

    /// <summary>
    /// A state install that fails at the second slot write of its snapshot has published the snapshot: a reopen finds the copy incomplete with its
    /// baseline taken from that snapshot, and a second transfer completes it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallSlotFaultReopensIncomplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-slot-fault");
        using var sourceDir = new TempDirectory("squirix-install-slot-fault-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
            faults.FailSnapshotSlotWrite(2);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancellationToken));
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).ReadPublishedAsync(cancellationToken)).IsNotNull();

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.MemberState).IsEqualTo(GroupMemberState.Incomplete);
        _ = await Assert.That(published.LastIncludedIndex).IsEqualTo(StateInstallScenario.SnapshotIndex);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((StateInstallScenario.SnapshotIndex, StateInstallScenario.SnapshotIndex));

        var retried = await FollowerSnapshotScenario.InstallAsync(reopened, snapshot, 1UL, cancellationToken);
        _ = await Assert.That(retried.Success).IsTrue().Because($"refused with '{retried.RefusalCode}'");
    }
}
