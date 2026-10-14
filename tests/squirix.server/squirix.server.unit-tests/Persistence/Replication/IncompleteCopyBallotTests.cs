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
using static Squirix.Server.UnitTests.Persistence.Replication.StateInstallLeaders;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>An incomplete copy keeps its term, its vote and its log, so it answers ballots like a complete one and never campaigns; an unseeded copy answers none.</summary>
[Immutable]
public sealed class IncompleteCopyBallotTests : ServerUnitTestBase
{
    private const string GroupId = StateInstallScenario.GroupId;

    /// <summary>An incomplete copy grants a pre-vote and a vote to a candidate with an up-to-date log and records the vote in both slots.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IncompleteCopyGrantsUpToDateCandidate(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-ballot-grant");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        var ballot = new ElectionVoteRequest("node-c", 5UL, 2UL, 1UL);

        var preVote = await log.CheckPreVoteAsync(ballot, cancellationToken);
        var vote = await log.RequestVoteAsync(ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode)).IsEqualTo((true, string.Empty));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((true, string.Empty, 5UL));
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, 5UL, "node-c"));
        var bytes = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        for (var slot = 0; slot < 2; slot++)
        {
            var record = StateInstallScenario.SlotRecord(bytes, slot);
            _ = await Assert.That((record.MemberState, record.CurrentTerm, record.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, 5UL, "node-c"));
        }
    }

    /// <summary>An incomplete copy refuses a candidate whose log is older than its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IncompleteCopyRefusesOlderLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-ballot-older");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        var ballot = new ElectionVoteRequest("node-c", 5UL, 1UL, 1UL);

        var preVote = await log.CheckPreVoteAsync(ballot, cancellationToken);
        var vote = await log.RequestVoteAsync(ballot, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.StaleLog));
        _ = await Assert.That((vote.Granted, vote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.StaleLog));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>An unseeded copy refuses a pre-vote and a vote, and an incomplete copy still blocks its own campaign.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededRefusesIncompleteNeverCampaigns(CancellationToken cancellationToken)
    {
        using var unseededDir = new TempDirectory("squirix-ballot-unseeded");
        using var incompleteDir = new TempDirectory("squirix-ballot-campaign");
        await using var unseeded = StateInstallScenario.NewLog(unseededDir);
        await unseeded.OpenAsync(cancellationToken);
        await using var incomplete = StateInstallScenario.NewLog(incompleteDir);
        await StateInstallScenario.OpenBehindAsync(incomplete, cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(incomplete, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
        var ballot = new ElectionVoteRequest("node-c", 5UL, 2UL, 1UL);

        var preVote = await unseeded.CheckPreVoteAsync(ballot, cancellationToken);
        var vote = await unseeded.RequestVoteAsync(ballot, cancellationToken);
        var status = await incomplete.GetStatusAsync(cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That((vote.Granted, vote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That(status.BlocksVote(status.CurrentTerm + 1UL)).IsTrue();
    }

    /// <summary>A copy under transfer votes in every phase: with the old log, and after a crash with the snapshot published, the log replaced and the mark still set.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VotesWorkInEachPhaseOfTheTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-ballot-phases");
        using var sourceDir = new TempDirectory("squirix-ballot-phases-source");
        var snapshot = await StateInstallScenario.LeaderSnapshotAsync(sourceDir, cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using (var log = StateInstallScenario.NewLog(dir, faults))
        {
            await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
            _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, StateInstallScenario.SnapshotIndex, 1UL, cancellationToken)).IsNull();
            var afterBegin = await log.RequestVoteAsync(new ElectionVoteRequest("node-c", 3UL, 2UL, 1UL), cancellationToken);
            _ = await Assert.That((afterBegin.Granted, afterBegin.CurrentTerm)).IsEqualTo((true, 3UL));

            faults.FailSlotWrite(1);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompleteStateInstallAsync(snapshot, Leader(3UL), cancellationToken));
        }

        await using var reopened = StateInstallScenario.NewLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.LastLogIndex, status.LastLogTerm, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Incomplete, 3UL, 1UL, 3UL, "node-c"));

        var stale = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-d", 4UL, 2UL, 1UL), cancellationToken);
        var current = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-d", 4UL, 3UL, 1UL), cancellationToken);

        _ = await Assert.That((stale.Granted, stale.RefusalCode, stale.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.StaleLog, 4UL));
        _ = await Assert.That((current.Granted, current.CurrentTerm)).IsEqualTo((true, 4UL));
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        var bytes = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        _ = await Assert.That((StateInstallScenario.SlotRecord(bytes, 0).VotedFor, StateInstallScenario.SlotRecord(bytes, 1).VotedFor)).IsEqualTo(("node-d", "node-d"));
    }
}
