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

/// <summary>
/// A copy that lost its disk takes a state transfer from a leader that checked the term ceiling (#1126): it stays unseeded, and votes for
/// nobody, until the end, which records the leader's term and a vote for the leader with the complete state in both metadata slots.
/// </summary>
[Immutable]
public sealed class UnseededRepairLogTests : ServerUnitTestBase
{
    private const ulong LeaderTerm = 3UL;

    /// <summary>
    /// The begin writes nothing and the copy stays unseeded at term zero; the end makes it complete at the snapshot, with the leader's term and a
    /// vote for the leader in each metadata slot alone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CeilingRepairRecordsVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-repair");
        using var sourceDir = new TempDirectory("squirix-unseeded-repair-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using (var log = StateInstallScenario.NewLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            var begin = await BeginAsync(log, in snapshot, cancellationToken);
            var during = await log.GetStatusAsync(cancellationToken);
            var complete = await log.CompleteStateInstallAsync(snapshot, CeilingLeader(LeaderTerm), cancellationToken);

            _ = await Assert.That(begin).IsNull();
            _ = await Assert.That((during.MemberState, during.CurrentTerm, during.VotedFor)).IsEqualTo((GroupMemberState.Unseeded, 0UL, string.Empty));
            _ = await Assert.That(complete.Success).IsTrue();
        }

        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, StateInstallScenario.GroupId), cancellationToken);
        for (var slot = 0; slot < 2; slot++)
        {
            var record = StateInstallScenario.SlotRecord(bytes, slot);
            _ = await Assert.That((record.MemberState, record.CurrentTerm, record.VotedFor, record.CommitIndex))
                            .IsEqualTo((GroupMemberState.Complete, LeaderTerm, LeaderNodeId, StateInstallScenario.SnapshotIndex));
        }
    }

    /// <summary>
    /// A copy that voted, lost its disk and was repaired in the same term refuses another candidate in that term; the leader's own request is a
    /// replay, and a later term is open again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepairedCopyVotesOnceInTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-repair-vote");
        using var sourceDir = new TempDirectory("squirix-unseeded-repair-vote-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using var log = StateInstallScenario.NewLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await BeginAsync(log, in snapshot, cancellationToken);
        _ = await log.CompleteStateInstallAsync(snapshot, CeilingLeader(LeaderTerm), cancellationToken);
        var (index, term) = (StateInstallScenario.SnapshotIndex, snapshot.LastIncludedTerm);

        var other = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", LeaderTerm, index, term), cancellationToken);
        var replay = await log.RequestVoteAsync(new ElectionVoteRequest(LeaderNodeId, LeaderTerm, index, term), cancellationToken);
        var later = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", LeaderTerm + 1UL, index, term), cancellationToken);

        _ = await Assert.That((other.Granted, other.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.AlreadyVoted));
        _ = await Assert.That(replay.Granted).IsTrue();
        _ = await Assert.That(later.Granted).IsTrue();
    }

    /// <summary>A transfer cut short after its begin leaves the copy unseeded after a restart: it still refuses votes and adopts no term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutRepairLeavesCopyUnseeded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-repair-cut");
        using var sourceDir = new TempDirectory("squirix-unseeded-repair-cut-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        await using (var log = StateInstallScenario.NewLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That(await BeginAsync(log, in snapshot, cancellationToken)).IsNull();
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var vote = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-b", LeaderTerm, 0UL, 0UL), cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That((vote.Granted, vote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Unseeded, 0UL, string.Empty));
    }

    /// <summary>
    /// A crash after the snapshot is published but before the complete record leaves the copy unseeded after a restart, refusing votes; the next
    /// transfer of a leader that checked the term ceiling completes it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrashBeforeRecordStaysUnseeded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-repair-crash");
        using var sourceDir = new TempDirectory("squirix-unseeded-repair-crash-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await BeginAsync(log, in snapshot, cancellationToken);
            faults.FailSlotWrite(1);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, CeilingLeader(LeaderTerm), cancellationToken));
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var vote = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-b", LeaderTerm, 0UL, 0UL), cancellationToken);
        var begin = await BeginAsync(reopened, in snapshot, cancellationToken);
        var complete = await reopened.CompleteStateInstallAsync(snapshot, CeilingLeader(LeaderTerm), cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That((vote.Granted, vote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That(begin).IsNull();
        _ = await Assert.That(complete.Success).IsTrue();
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Complete, LeaderTerm, LeaderNodeId));
    }

    private static Task<string?> BeginAsync(FollowerLog log, in GroupSnapshot snapshot, CancellationToken cancellationToken) =>
        log.BeginStateInstallAsync(CeilingLeader(LeaderTerm), snapshot.LastIncludedIndex, snapshot.TopologyFingerprint, snapshot.ConfigurationGeneration, snapshot, cancellationToken);
}
