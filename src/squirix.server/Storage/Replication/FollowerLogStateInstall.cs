using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Operations of a state transfer: marks a copy incomplete, completes it at a snapshot, and captures the snapshot a leader sends.</summary>
/// <remarks>
/// <para>
/// The caller holds the log gate. Every metadata record of a transfer is published to both slots through the mirrored path, like the member
/// state changes of the first start, so damage to one slot cannot roll the mark back. Recovery never writes the member state: it keeps whatever
/// the newest valid record holds.
/// </para>
/// <para>
/// An unseeded copy, which lost its disk, takes a transfer only from a leader that checked the term ceiling (#1126). It stays unseeded, and
/// votes for nobody, for the whole transfer: the begin writes nothing. The completion writes, in its one complete record, the leader's term
/// and a vote for the leader, so the copy cannot vote a second time in a term it may have voted in before the loss. A transfer cut short
/// leaves the copy unseeded.
/// </para>
/// </remarks>
internal static class FollowerLogStateInstall
{
    internal static async Task<string?> BeginAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        bool open,
        bool topologyMismatch,
        (StateInstallLeader Leader, ulong Index, GroupSnapshot Snapshot) transfer,
        CancellationToken cancellationToken)
    {
        // The snapshot is checked before the copy is marked, so one the completion would refuse costs the copy nothing.
        var meta = owner.Meta;
        var refusal = (open, topologyMismatch) switch
        {
            (false, _) => FollowerLogRefusal.NotReady,
            (true, true) => FollowerLogRefusal.TopologyMismatch,
            _ => RefuseTransfer(in meta, in transfer.Leader, transfer.Index) ?? RefuseSnapshot(journal, owner, in transfer.Snapshot),
        };
        if (refusal != null || meta.MemberState == GroupMemberState.Unseeded)
            return refusal;

        // A higher leader term is adopted with the vote cleared, any other term keeps the term and the vote.
        var leaderTerm = transfer.Leader.Term;
        var candidate = leaderTerm > meta.CurrentTerm ? meta with { CurrentTerm = leaderTerm, VotedFor = string.Empty } : meta;
        await StoreAsync(journal, owner, candidate with { MemberState = GroupMemberState.Incomplete }, cancellationToken).ConfigureAwait(false);
        return null;
    }

    internal static GroupSnapshotCapture Capture(FollowerLogJournal journal, IFollowerLogContext owner, bool open)
    {
        var index = owner.Meta.LastAppliedIndex;
        var outcome = (open, owner.Meta.MemberState == GroupMemberState.Complete, index == 0UL) switch
        {
            (false, _, _) => GroupSnapshotCaptureOutcome.NotReady,
            (true, false, _) => GroupSnapshotCaptureOutcome.NotComplete,
            (true, true, true) => GroupSnapshotCaptureOutcome.NothingApplied,
            _ when owner.Idempotency.HasUnresolvedThrough(index) => GroupSnapshotCaptureOutcome.UnresolvedOutcome,
            _ => GroupSnapshotCaptureOutcome.Captured,
        };
        return outcome == GroupSnapshotCaptureOutcome.Captured
            ? new GroupSnapshotCapture(outcome, FollowerLogSnapshot.BuildSnapshot(journal, owner, index))
            : GroupSnapshotCapture.Refused(outcome);
    }

    internal static async Task<GroupSnapshotInstallResult> CompleteAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        bool open,
        GroupSnapshot snapshot,
        StateInstallLeader leader,
        CancellationToken cancellationToken)
    {
        var refusal = open ? RefuseCompletion(owner, in snapshot, in leader) : FollowerLogRefusal.NotReady;
        if (refusal != null)
            return GroupSnapshotInstallResult.Refused(refusal);

        // The snapshot is published first. If that fails before its first slot is flushed, the old snapshot, metadata and log remain authoritative;
        // a failure after that point may leave the new snapshot published, and a failure of any later step leaves the copy incomplete, or unseeded,
        // so recovery restores the baseline from the snapshot on the next open.
        await FollowerLogSnapshot.PublishOrFailReadinessAsync(journal, owner, snapshot, cancellationToken).ConfigureAwait(false);
        await ReplaceStateAsync(journal, owner, snapshot, leader).ConfigureAwait(false);
        return GroupSnapshotInstallResult.Installed;
    }

    private static async Task ReplaceStateAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, StateInstallLeader leader)
    {
        try
        {
            // Past the publish there is no way back: a caller's cancellation must not fail the log half way through.
            await ReplaceLogAndStoreAsync(journal, owner, snapshot, leader, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // The in-memory state may trail the durable one, so the log is never surfaced as ready after a failed step.
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    private static async Task ReplaceLogAndStoreAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        GroupSnapshot snapshot,
        StateInstallLeader leader,
        CancellationToken cancellationToken)
    {
        // Both restores precede the log replacement, so a fault during it cannot leave the in-memory map holding entries the transfer
        // discarded. The restore keeps every committed outcome whatever the idempotency capacity, which bounds new reservations only.
        owner.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
        owner.Idempotency.RestoreFromSnapshot(snapshot.CommittedOutcomes, snapshot.CapturedUtc, []);

        // The log holds nothing above the baseline: the transferred state is the committed prefix, and the leader sends what follows.
        var (length, _) = await FollowerLogDurable.ReplaceLogAsync(journal, owner, [], cancellationToken).ConfigureAwait(false);
        journal.ClearEntries();
        owner.LastLogIndex = snapshot.LastIncludedIndex;
        owner.LogLength = length;

        // The complete record is the last step. A higher leader term is adopted with it, with the vote cleared; a copy that was unseeded
        // takes the leader's term with a vote for the leader, since it may have voted in that term before it lost its disk.
        var meta = owner.Meta;
        var adopted = AdoptedTerm(in meta, in leader);
        var complete = adopted with
        {
            MemberState = GroupMemberState.Complete,
            CommitIndex = snapshot.LastIncludedIndex,
            LastAppliedIndex = snapshot.LastIncludedIndex,
            TopologyFingerprint = meta.TopologyFingerprint.IsEmpty ? snapshot.TopologyFingerprint : meta.TopologyFingerprint,
            ConfigurationGeneration = Math.Max(meta.ConfigurationGeneration, snapshot.ConfigurationGeneration),
        };
        await StoreAsync(journal, owner, complete, cancellationToken).ConfigureAwait(false);
    }

    private static GroupLogMetadata AdoptedTerm(in GroupLogMetadata meta, in StateInstallLeader leader) => true switch
    {
        _ when meta.MemberState == GroupMemberState.Unseeded => meta with { CurrentTerm = leader.Term, VotedFor = leader.NodeId },
        _ when leader.Term > meta.CurrentTerm => meta with { CurrentTerm = leader.Term, VotedFor = string.Empty },
        _ => meta,
    };

    private static string? RefuseTransfer(in GroupLogMetadata meta, in StateInstallLeader leader, ulong index)
    {
        return true switch
        {
            _ when leader.Term < meta.CurrentTerm => FollowerLogRefusal.StaleTerm,
            _ when meta.MemberState == GroupMemberState.Unseeded && !leader.CeilingChecked => FollowerLogRefusal.Unseeded,
            _ when index < meta.CommitIndex => FollowerLogRefusal.NotReady,
            _ => null,
        };
    }

    private static string? RefuseSnapshot(FollowerLogJournal journal, IFollowerLogContext owner, in GroupSnapshot snapshot)
    {
        var topologyMismatch = FollowerLogSnapshot.SnapshotTopologyMismatch(owner, in snapshot);
        return true switch
        {
            _ when !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal) => FollowerLogRefusal.NotMember,
            _ when topologyMismatch != null => topologyMismatch,
            _ when IsMalformed(owner, in snapshot) || !journal.Snapshot.Fits(snapshot) => FollowerLogRefusal.NotReady,
            _ => null,
        };
    }

    private static string? RefuseCompletion(IFollowerLogContext owner, in GroupSnapshot snapshot, in StateInstallLeader leader)
    {
        var topologyMismatch = FollowerLogSnapshot.SnapshotTopologyMismatch(owner, in snapshot);
        var state = owner.Meta.MemberState;
        return true switch
        {
            _ when state == GroupMemberState.Unseeded && !leader.CeilingChecked => FollowerLogRefusal.Unseeded,
            _ when state == GroupMemberState.Complete => FollowerLogRefusal.NotReady,
            _ when !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal) => FollowerLogRefusal.NotMember,
            _ when topologyMismatch != null => topologyMismatch,
            _ when leader.Term < owner.Meta.CurrentTerm => FollowerLogRefusal.StaleTerm,
            _ when IsMalformed(owner, in snapshot) => FollowerLogRefusal.NotReady,
            _ => null,
        };
    }

    private static bool IsMalformed(IFollowerLogContext owner, in GroupSnapshot snapshot)
    {
        // Terms start at 1, and a zero baseline collides with the "unverifiable term" sentinel and would make recovery discard the whole
        // durable suffix. The transferred state is authoritative for the covered prefix, so one below the durable commit index is refused:
        // durable watermarks never move back. A commit index below the included index is internally inconsistent.
        var baselineInvalid = snapshot.LastIncludedIndex == 0UL || snapshot.LastIncludedTerm == 0UL || snapshot.CommitIndex < snapshot.LastIncludedIndex;
        var belowCommit = snapshot.LastIncludedIndex < owner.Meta.CommitIndex;

        // A snapshot with an unresolved outcome would write invalid idempotency state to disk and poison the next recovery, and so would
        // an outcome beyond the boundary, which a direct snapshot bypassing the on-disk decoder can carry. Both are refused before anything
        // durable is written.
        return baselineInvalid || belowCommit || HasUnresolvedOutcome(snapshot.CommittedOutcomes) || HasOutcomeBeyond(snapshot.CommittedOutcomes, snapshot.LastIncludedIndex);
    }

    private static bool HasOutcomeBeyond(IReadOnlyList<GroupIdempotencyRecord> outcomes, ulong lastIncludedIndex)
    {
        for (var i = 0; i < outcomes.Count; i++)
        {
            if (outcomes[i].LogIndex > lastIncludedIndex)
                return true;
        }

        return false;
    }

    private static bool HasUnresolvedOutcome(IReadOnlyList<GroupIdempotencyRecord> outcomes)
    {
        for (var i = 0; i < outcomes.Count; i++)
        {
            if (outcomes[i].ResolvedUtc == null)
                return true;
        }

        return false;
    }

    private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
    {
        await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
    }
}
