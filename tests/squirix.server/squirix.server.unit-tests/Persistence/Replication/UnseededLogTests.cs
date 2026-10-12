using System;
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

/// <summary>A copy that is not complete: how a fresh log opens, what it keeps on disk, and that it refuses every call that would change or answer for the group.</summary>
[Immutable]
public sealed class UnseededLogTests : ServerUnitTestBase
{
    private const string GroupId = "unseeded-log";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A fresh log holds the unseeded record in both metadata slots, so each slot alone reads it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshStateIsInBothSlots(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-slots");
        await using (var log = OpenLog(dir))
            await log.OpenAsync(cancellationToken);

        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

        for (var slot = 0; slot < 2; slot++)
        {
            var alone = new byte[bytes.Length];
            bytes.CopyTo(alone, 0);
            Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);

            var read = GroupMetadataSlots.Select(alone);

            _ = await Assert.That((read.Status, read.Slot, read.Metadata.MemberState, read.Metadata.BootstrapOffered)).IsEqualTo((GroupMetadataReadStatus.Valid, slot, GroupMemberState.Unseeded, false));
        }
    }

    /// <summary>A reopen reads the fresh record back: still unseeded, not offered, and ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshStateSurvivesReopen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-reopen");
        await using (var log = OpenLog(dir))
            await log.OpenAsync(cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.BootstrapOffered, status.Readiness)).IsEqualTo((GroupMemberState.Unseeded, false, FollowerLogReadiness.Ready));
    }

    /// <summary>Damage to either slot of the fresh record leaves the other one, which still reads unseeded.</summary>
    /// <param name="slot">The slot that is damaged.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task FreshStateSurvivesSlotDamage(int slot, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-damage");
        await using (var log = OpenLog(dir))
            await log.OpenAsync(cancellationToken);

        await FollowerLogTestKit.CorruptByteAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), (slot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.BootstrapOffered, status.Readiness)).IsEqualTo((GroupMemberState.Unseeded, false, FollowerLogReadiness.Ready));
    }

    /// <summary>A published snapshot without metadata or log fails readiness: the vote memory of the group is gone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotWithoutMetadataFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-snapshot");
        var snapshotPath = GroupStoragePaths.GetSnapshotPath(dir, GroupId);
        _ = Directory.CreateDirectory(Path.GetDirectoryName(snapshotPath)!);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);
        await new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken);

        await using var log = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("metadata is missing while the snapshot file exists", StringComparison.Ordinal);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetMetadataPath(dir, GroupId))).IsFalse();
    }

    /// <summary>An unseeded log refuses a heartbeat, a probe and an append with entries, reporting term and last index zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendsAreRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-append");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var heartbeat = await log.AppendAsync(new FollowerLogAppendRequest("leader", 3UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
        var probe = await log.AppendAsync(new FollowerLogAppendRequest("leader", 3UL, 5UL, 2UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
        var entries = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 3UL, "a"), cancellationToken);
        var topology = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 3UL, "a"), Fingerprint, 1UL, cancellationToken);

        _ = await Assert.That((heartbeat.Success, heartbeat.RefusalCode, heartbeat.CurrentTerm, heartbeat.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL, 0UL));
        _ = await Assert.That((probe.Success, probe.RefusalCode, probe.CurrentTerm, probe.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL, 0UL));
        _ = await Assert.That((entries.Success, entries.RefusalCode, entries.CurrentTerm, entries.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL, 0UL));
        _ = await Assert.That((topology.Success, topology.RefusalCode, topology.CurrentTerm, topology.LastLogIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL, 0UL));
    }

    /// <summary>An unseeded log refuses both commit advances and the applied advance.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitAndAppliedAreRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-commit");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var local = await log.AdvanceCommitAsync(1UL, cancellationToken);
        var leader = await log.AdvanceCommitAsync(1UL, 3UL, cancellationToken);
        var applied = await log.AdvanceAppliedAsync(1UL, cancellationToken);

        _ = await Assert.That((local.Success, local.RefusalCode, local.CommitIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That((leader.Success, leader.RefusalCode, leader.CommitIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That((applied.Success, applied.RefusalCode, applied.AppliedIndex)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
    }

    /// <summary>An unseeded log refuses a snapshot install, a tail reconcile and a compaction.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallReconcileAndCompactionAreRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-install");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);

        var install = await log.InstallSnapshotAsync(snapshot, 3UL, cancellationToken);
        var reconcile = await log.ReconcileTailAsync(1UL, 0UL, 3UL, cancellationToken);
        var compaction = await log.CompactThroughAsync(1UL, cancellationToken);

        _ = await Assert.That((install.Success, install.Refusal)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That((reconcile.Success, reconcile.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.Unseeded));
        _ = await Assert.That(compaction).IsEqualTo(GroupCompactionOutcome.NotReady);
    }

    /// <summary>An unseeded log answers neither a pre-vote nor a vote request, and adopts no term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VotesAndTermsAreRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-votes");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        var ballot = new ElectionVoteRequest("node-b", 5UL, 0UL, 0UL);

        var preVote = await log.CheckPreVoteAsync(ballot, cancellationToken);
        var vote = await log.RequestVoteAsync(ballot, cancellationToken);
        var observed = await log.ObserveTermAsync(7UL, cancellationToken);

        _ = await Assert.That((preVote.Granted, preVote.RefusalCode, preVote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.Unseeded, 0UL));
        _ = await Assert.That(observed).IsEqualTo(0UL);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((0UL, string.Empty));
    }

    /// <summary>Every refusal persists nothing: the metadata file keeps its bytes, and the log keeps term zero and no vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalsLeaveMetadataUntouched(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-bytes");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);

        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 3UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, 3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);
        _ = await log.InstallSnapshotAsync(snapshot, 3UL, cancellationToken);
        _ = await log.ReconcileTailAsync(1UL, 0UL, 3UL, cancellationToken);
        _ = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-b", 5UL, 0UL, 0UL), cancellationToken);
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 5UL, 0UL, 0UL), cancellationToken);
        _ = await log.ObserveTermAsync(7UL, cancellationToken);

        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, status.LastLogIndex, status.CommitIndex)).IsEqualTo((0UL, string.Empty, 0UL, 0UL));
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
}
