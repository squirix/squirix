using System;
using System.IO;
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

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Marking a copy incomplete at the start of a state transfer: what it refuses, what it writes, and what a crash between its slot writes leaves.</summary>
[Immutable]
public sealed class StateInstallBeginTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A log that is not open refuses the begin as not ready and writes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusesLogThatIsNotOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-not-open");
        await using var log = StateInstallScenario.NewLog(dir);

        var refusal = await log.BeginStateInstallAsync(1UL, 1UL, ReadOnlyMemory<byte>.Empty, 0UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetMetadataPath(dir, StateInstallScenario.GroupId))).IsFalse();
    }

    /// <summary>A leader of another topology or an older generation is refused, and the metadata keeps its bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusesTopologyMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-topology");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        await log.AdoptTopologyAsync(Fingerprint, 2UL, cancellationToken);
        var before = await ReadMetaAsync(dir, cancellationToken);

        var other = await log.BeginStateInstallAsync(1UL, 2UL, new byte[] { 4 }, 2UL, cancellationToken);
        var older = await log.BeginStateInstallAsync(1UL, 2UL, Fingerprint, 1UL, cancellationToken);

        _ = await Assert.That((other, older)).IsEqualTo((FollowerLogRefusal.TopologyMismatch, FollowerLogRefusal.TopologyMismatch));
        await AssertUnchangedAsync(dir, before, cancellationToken);
        _ = await Assert.That(log.IsComplete).IsTrue();
    }

    /// <summary>A leader term below the durable term is refused, and the metadata keeps its bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusesStaleTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-stale");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var before = await ReadMetaAsync(dir, cancellationToken);

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, 2UL, 2UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.StaleTerm);
        await AssertUnchangedAsync(dir, before, cancellationToken);
    }

    /// <summary>An unseeded copy is not repaired by this path: the begin is refused as unseeded and the metadata keeps its bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusesUnseededCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-unseeded");
        await using var log = StateInstallScenario.NewLog(dir);
        await log.OpenAsync(cancellationToken);
        var before = await ReadMetaAsync(dir, cancellationToken);

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, 1UL, 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.Unseeded);
        await AssertUnchangedAsync(dir, before, cancellationToken);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>An index below the durable commit index is refused as not ready: durable watermarks never move back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusesIndexBelowCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-below-commit");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        var before = await ReadMetaAsync(dir, cancellationToken);

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, 1UL, 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.NotReady);
        await AssertUnchangedAsync(dir, before, cancellationToken);
        _ = await Assert.That(log.IsComplete).IsTrue();
    }

    /// <summary>An accepted begin holds the incomplete record in both slots, and leaves the log, the commit index and the applied index as they were.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginMarksBothSlotsAndKeepsTheLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-marks");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
        var logBefore = await StateInstallScenario.ReadLogBytesAsync(dir, cancellationToken);
        var statusBefore = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.IsComplete).IsTrue();

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, 3UL, 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsNull();
        _ = await Assert.That(log.IsComplete).IsFalse();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, statusBefore.CurrentTerm, statusBefore.VotedFor));
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((statusBefore.LastLogIndex, statusBefore.CommitIndex, statusBefore.LastAppliedIndex));
        var logAfter = await StateInstallScenario.ReadLogBytesAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(logBefore, logAfter);
        var bytes = await ReadMetaAsync(dir, cancellationToken);
        _ = await Assert.That(StateInstallScenario.SlotStates(bytes)).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete));
    }

    /// <summary>A higher leader term is adopted with the vote cleared; the same term keeps the term and the vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginAdoptsHigherTermAndClearsVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-term");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 2UL, 1UL), cancellationToken);

        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, 2UL, 2UL, cancellationToken)).IsNull();
        var same = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, 2UL, 4UL, cancellationToken)).IsNull();
        var higher = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That((same.CurrentTerm, same.VotedFor)).IsEqualTo((2UL, "node-b"));
        _ = await Assert.That((higher.CurrentTerm, higher.VotedFor, higher.MemberState)).IsEqualTo((4UL, string.Empty, GroupMemberState.Incomplete));
        var bytes = await ReadMetaAsync(dir, cancellationToken);
        _ = await Assert.That((StateInstallScenario.SlotRecord(bytes, 0).CurrentTerm, StateInstallScenario.SlotRecord(bytes, 1).CurrentTerm)).IsEqualTo((4UL, 4UL));
    }

    /// <summary>A copy that is incomplete already accepts the begin again, which publishes the record to both slots once more.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedBeginIsAccepted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-repeated");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);

        var first = await FollowerSnapshotScenario.BeginAsync(log, 3UL, 1UL, cancellationToken);
        var second = await FollowerSnapshotScenario.BeginAsync(log, 3UL, 1UL, cancellationToken);

        _ = await Assert.That(first).IsNull();
        _ = await Assert.That(second).IsNull();
        _ = await Assert.That(StateInstallScenario.SlotStates(await ReadMetaAsync(dir, cancellationToken))).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete));
        _ = await Assert.That(log.IsComplete).IsFalse();
    }

    /// <summary>A fault at either slot write fails readiness and leaves a legal state on reopen, complete as before or incomplete, with the log untouched.</summary>
    /// <param name="failedWrite">The slot write that fails: the first or the second.</param>
    /// <param name="reopenedIncomplete">Whether a reopen reads the incomplete record.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, false)]
    [Arguments(2, true)]
    public async Task BeginFaultLeavesLegalState(int failedWrite, bool reopenedIncomplete, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-fault");
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            faults.FailSlotWrite(failedWrite);

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(FollowerSnapshotScenario.BeginAsync(log, 2UL, 1UL, cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.MemberState == GroupMemberState.Incomplete).IsEqualTo(reopenedIncomplete);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((2UL, 2UL, 2UL));
        _ = await Assert.That(await reopened.GetTermAtAsync(2UL, cancellationToken)).IsEqualTo(1UL);

        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(reopened, 2UL, 1UL, cancellationToken)).IsNull();
        _ = await Assert.That(StateInstallScenario.SlotStates(await ReadMetaAsync(dir, cancellationToken))).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete));
    }

    /// <summary>The complete flag read without the gate follows the transfer: set while complete, clear from the begin, set again by the completion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteFlagFollowsTheTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-begin-flag");
        using var sourceDir = new TempDirectory("squirix-begin-flag-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        var complete = log.IsComplete;

        _ = await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken);
        var incomplete = log.IsComplete;
        var result = await log.CompleteStateInstallAsync(snapshot, 1UL, cancellationToken);

        _ = await Assert.That((complete, incomplete, result.Success, log.IsComplete)).IsEqualTo((true, false, true, true));
    }

    private static async Task AssertUnchangedAsync(TempDirectory dir, byte[] before, CancellationToken cancellationToken)
    {
        var after = await ReadMetaAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    private static Task<byte[]> ReadMetaAsync(TempDirectory dir, CancellationToken cancellationToken) =>
        GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, StateInstallScenario.GroupId), cancellationToken);
}
