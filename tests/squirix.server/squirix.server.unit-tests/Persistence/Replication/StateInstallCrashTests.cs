using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Persistence.Replication.StateInstallLeaders;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Completing a state transfer and crashing in the middle of it: the member state is only ever changed by the transfer, never by recovery.</summary>
[Immutable]
public sealed class StateInstallCrashTests : ServerUnitTestBase
{
    private const string GroupId = StateInstallScenario.GroupId;

    /// <summary>The point of a state transfer at which the node crashes.</summary>
    public enum CrashPoint
    {
        /// <summary>After the begin, before the snapshot is durable.</summary>
        BeforeSnapshot = 0,

        /// <summary>The snapshot is durable, the log replacement is not.</summary>
        BeforeLog = 1,

        /// <summary>The snapshot and the replaced log are durable, the complete record is not.</summary>
        BeforeRecord = 2,

        /// <summary>The complete record reached the first slot only.</summary>
        BetweenSlots = 3,
    }

    /// <summary>A completed transfer holds the complete record in both slots at the snapshot index and takes the next entry of the leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedTransferIsCompleteAtSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-complete");
        using var sourceDir = new TempDirectory("squirix-transfer-complete-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);

        var result = await FollowerSnapshotScenario.InstallAsync(log, snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue().Because($"refused with '{result.RefusalCode}'");
        _ = await Assert.That(log.IsComplete).IsTrue();
        var bytes = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        for (var slot = 0; slot < 2; slot++)
        {
            var record = StateInstallScenario.SlotRecord(bytes, slot);
            _ = await Assert.That((record.MemberState, record.CommitIndex, record.LastAppliedIndex, record.CurrentTerm)).IsEqualTo((GroupMemberState.Complete, 3UL, 3UL, 1UL));
        }

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 1UL, 3UL, 3UL));
        _ = await Assert.That(await log.GetTermAtAsync(3UL, cancellationToken)).IsEqualTo(1UL);
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That((published.LastIncludedIndex, published.LastIncludedTerm)).IsEqualTo((3UL, 1UL));
        _ = await Assert.That(log.Idempotency.Lookup("client", "operation-1", [1, 2, 3], out var outcome)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(outcome.OutcomePayload.Span is [9]).IsTrue();
        var next = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 4UL, 1UL, "d"), cancellationToken);
        _ = await Assert.That((next.Success, next.LastLogIndex)).IsEqualTo((true, 4UL));
    }

    /// <summary>
    /// A crash at each point of the transfer reopens incomplete until the complete record is read, refuses an append and a probe as incomplete,
    /// and a second transfer then ends complete with the snapshot data.
    /// </summary>
    /// <param name="point">The crash point.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(CrashPoint.BeforeSnapshot)]
    [Arguments(CrashPoint.BeforeLog)]
    [Arguments(CrashPoint.BeforeRecord)]
    [Arguments(CrashPoint.BetweenSlots)]
    public async Task CrashKeepsIncompleteUntilRecordIsRead(CrashPoint point, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-crash");
        using var sourceDir = new TempDirectory("squirix-transfer-crash-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
            await CrashAsync(log, faults, snapshot, point, cancellationToken);
        }

        var reopenedComplete = point == CrashPoint.BetweenSlots;
        await using (var reopened = StateInstallScenario.NewLog(dir))
        {
            await reopened.OpenAsync(cancellationToken);
            var status = await reopened.GetStatusAsync(cancellationToken);
            var snapshotPublished = new GroupSnapshotStore(dir, GroupId).SnapshotExists;

            _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
            _ = await Assert.That((status.MemberState == GroupMemberState.Complete, reopened.IsComplete)).IsEqualTo((reopenedComplete, reopenedComplete));
            _ = await Assert.That(snapshotPublished).IsEqualTo(point != CrashPoint.BeforeSnapshot);
            _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo(point == CrashPoint.BeforeSnapshot ? (2UL, 2UL) : (3UL, 3UL));
            _ = await Assert.That(status.LastLogIndex).IsEqualTo(point is CrashPoint.BeforeSnapshot or CrashPoint.BeforeLog ? 2UL : 3UL);
            if (!reopenedComplete)
                await AssertAppendsRefusedAsync(reopened, status.LastLogIndex, cancellationToken);
        }

        await using var again = StateInstallScenario.NewLog(dir);
        await again.OpenAsync(cancellationToken);
        if (!reopenedComplete)
        {
            var retried = await FollowerSnapshotScenario.InstallAsync(again, snapshot, 1UL, cancellationToken);
            _ = await Assert.That(retried.Success).IsTrue().Because($"refused with '{retried.RefusalCode}'");
        }

        var final = await again.GetStatusAsync(cancellationToken);
        _ = await Assert.That((final.MemberState, final.LastLogIndex, final.CommitIndex, final.LastAppliedIndex)).IsEqualTo((GroupMemberState.Complete, 3UL, 3UL, 3UL));
        _ = await Assert.That(again.Idempotency.Lookup("client", "operation-1", [1, 2, 3], out var outcome)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(outcome.OutcomePayload.Span is [9]).IsTrue();
        _ = await Assert.That(await again.GetTermAtAsync(3UL, cancellationToken)).IsEqualTo(1UL);
        var next = await again.AppendAsync(FollowerFoundationScenario.Append("leader", 4UL, 1UL, "d"), cancellationToken);
        _ = await Assert.That(next.Success).IsTrue();
    }

    /// <summary>The state of a snapshot published beside an old log and an incomplete record is never changed by recovery, which raises only the watermarks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryNeverWritesComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-recovery");
        using var sourceDir = new TempDirectory("squirix-transfer-recovery-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using (var log = StateInstallScenario.NewLog(dir))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        }

        await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken);

        for (var round = 0; round < 2; round++)
        {
            await using var reopened = StateInstallScenario.NewLog(dir);
            await reopened.OpenAsync(cancellationToken);
            var durable = await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

            _ = await Assert.That((durable.MemberState, durable.CommitIndex, durable.LastAppliedIndex)).IsEqualTo((GroupMemberState.Incomplete, 3UL, 3UL));
            _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
            await AssertAppendsRefusedAsync(reopened, 2UL, cancellationToken);
        }
    }

    /// <summary>
    /// A snapshot published beside an old log that is longer than the snapshot index leaves the copy incomplete, whether the old log holds the
    /// term of the snapshot at that index or another one, and the copy refuses appends.
    /// </summary>
    /// <param name="diverging">Whether the old log holds another term than the snapshot at the snapshot index.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LongerOldLogKeepsCopyIncomplete(bool diverging, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-longer-log");
        using var sourceDir = new TempDirectory("squirix-transfer-longer-log-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var term = diverging ? 2UL : 1UL;
        await using (var log = StateInstallScenario.NewLog(dir))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            var beyond = new[] { new FollowerLogEntry(3UL, term, Encoding.UTF8.GetBytes("c")), new FollowerLogEntry(4UL, term, Encoding.UTF8.GetBytes("d")) };
            _ = await log.AppendAsync(new FollowerLogAppendRequest("leader", term, 2UL, 1UL, 0UL, beyond), cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, term, cancellationToken)).IsNull();
        }

        await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken);

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var durable = await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That((reopened.Readiness, durable.MemberState, status.MemberState)).IsEqualTo((FollowerLogReadiness.Ready, GroupMemberState.Incomplete, GroupMemberState.Incomplete));
        var append = await reopened.AppendAsync(new FollowerLogAppendRequest("leader", term, status.LastLogIndex, status.LastLogTerm, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
        _ = await Assert.That((append.Success, append.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Incomplete));
    }

    /// <summary>Damage to either slot after a durable begin does not remove the mark.</summary>
    /// <param name="damagedSlot">The slot that is damaged.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task SlotDamageKeepsTheMark(int damagedSlot, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-damage");
        await using (var log = StateInstallScenario.NewLog(dir))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        }

        await FollowerLogTestKit.CorruptByteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), (damagedSlot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.Readiness)).IsEqualTo((GroupMemberState.Incomplete, FollowerLogReadiness.Ready));
        await AssertAppendsRefusedAsync(reopened, 2UL, cancellationToken);
    }

    /// <summary>A completion with a fault at the first slot write fails readiness, and the copy is not reported complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedCompletionFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-failed");
        using var sourceDir = new TempDirectory("squirix-transfer-failed-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using var log = StateInstallScenario.NewLog(dir, faults);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        faults.FailSlotWrite(1);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancellationToken));

        _ = await Assert.That((log.Readiness, log.IsComplete)).IsEqualTo((FollowerLogReadiness.Failed, false));
    }

    private static async Task CrashAsync(FollowerLog log, StateInstallFaultHooks faults, GroupSnapshot snapshot, CrashPoint point, CancellationToken cancellationToken)
    {
        switch (point)
        {
            case CrashPoint.BeforeSnapshot:
                return;
            case CrashPoint.BeforeLog:
                faults.FailNextFlush();
                break;
            case CrashPoint.BeforeRecord:
                faults.FailSlotWrite(1);
                break;
            case CrashPoint.BetweenSlots:
                faults.FailSlotWrite(2);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(point), point, "Unsupported crash point.");
        }

        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancellationToken));
    }

    private static async Task AssertAppendsRefusedAsync(FollowerLog log, ulong lastLogIndex, CancellationToken cancellationToken)
    {
        var append = await log.AppendAsync(FollowerFoundationScenario.Append("leader", lastLogIndex + 1UL, 1UL, "x"), cancellationToken);
        var probe = await log.AppendAsync(new FollowerLogAppendRequest("leader", 1UL, lastLogIndex, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);

        _ = await Assert.That((append.Success, append.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Incomplete));
        _ = await Assert.That((probe.Success, probe.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Incomplete));
    }
}
