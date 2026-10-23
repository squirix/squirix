using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The capture of the state a leader sends to a follower that cannot be caught up from its log, taken under the commit gate of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterTransfer
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Captures the group snapshot at the applied index of the led group and pins the log at it.</summary>
        /// <param name="target">The follower the state is for, with the pipeline and the term its catch-up runs in.</param>
        /// <param name="durability">The node cache journal whose frames the applies appended.</param>
        /// <param name="pin">The hold of the transfer attempt; it is taken at the captured index before the gate is left.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The snapshot with <see cref="ReplicaTransferOutcome.Transferred" />, or the reason nothing was captured and no hold was taken.</returns>
        /// <remarks>
        /// <para>
        /// One step under the commit gate: no write appends, commits or applies between the checks and the capture. The committer must be started
        /// on the pipeline of the target and lead its term: an elected leader holds a tenure of the term, the log is in it, and the election
        /// state says leader in it with no higher term observed; a statically led group passes the static term check. Nothing may wait to
        /// be applied, the outcomes of the log must be rebuilt, and the applied index is made durable (one flush) so the snapshot is taken
        /// at the applied index of memory. The capture itself refuses an index of zero, a copy that is not complete and an entry without a
        /// known outcome.
        /// </para>
        /// <para>
        /// Authority is not asked for: a leader whose only other live member needs the transfer cannot get authority without it. Everything
        /// captured is committed data, since the applied index never passes the commit index.
        /// </para>
        /// </remarks>
        internal async Task<(ReplicaTransferOutcome Outcome, GroupSnapshot Snapshot)> CaptureForTransferAsync(
            ReplicaCatchUpTarget target,
            IJournalDurabilityCoordinator durability,
            ReplicaTransferPin.Session pin,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(durability);
            ArgumentNullException.ThrowIfNull(pin);
            committer.ThrowIfDisposed();
            using var guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            if (!committer.Registry.TryGetLog(committer.GroupId, out var log) || !ReferenceEquals(committer.RunningPipeline, target.Pipeline) || committer.Coordinator is not { } coordinator)
                return (ReplicaTransferOutcome.NotLeading, default);

            var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            var refusal = committer.LeadershipRefusal(in status, target.Term) ?? await committer.ApplyRefusalAsync(log, (coordinator, status.CommitIndex), durability, cancellationToken).ConfigureAwait(false);
            if (refusal is { } refused)
                return (refused, default);

            var capture = await log.CaptureSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var outcome = CaptureOutcome(capture.Outcome);
            if (outcome != ReplicaTransferOutcome.Transferred)
                return (outcome, default);

            // The flush made the durable applied index the one of memory under the gate; anything else means an apply slipped in, and nothing is sent.
            if (capture.Snapshot.LastIncludedIndex != committer.Applier.AppliedIndex)
                return (ReplicaTransferOutcome.PendingApply, default);

            pin.Hold(capture.Snapshot.LastIncludedIndex);
            return (ReplicaTransferOutcome.Transferred, capture.Snapshot);
        }

        /// <summary>Flushes the applied index once nothing waits to be applied and the idempotency outcomes of the log are rebuilt.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="committed">The running coordinator and the durable commit index of the log.</param>
        /// <param name="durability">The node cache journal whose frames the applies appended.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The reason the state cannot be captured yet, or <see langword="null" /> once the durable applied index is the one of memory.</returns>
        /// <remarks>
        /// Runs under the commit gate. Nothing committed may wait to be applied: the applied index of memory is the commit index of the log and of
        /// the coordinator. An uncommitted tail, such as the leader-term entry of a leadership without authority, does not wait for an apply.
        /// </remarks>
        private async Task<ReplicaTransferOutcome?> ApplyRefusalAsync(IFollowerLog log, (ReplicaCommitCoordinator Coordinator, ulong CommitIndex) committed, IJournalDurabilityCoordinator durability, CancellationToken cancellationToken)
        {
            var applied = committer.Applier.AppliedIndex;
            if (committed.CommitIndex != applied || committed.Coordinator.CommitIndex != applied)
                return ReplicaTransferOutcome.PendingApply;

            if (!log.Idempotency.OutcomesRebuilt)
                return ReplicaTransferOutcome.UnresolvedOutcome;

            await committer.Applier.FlushAsync(log, durability, cancellationToken).ConfigureAwait(false);
            return null;
        }

        /// <summary>Tells whether this committer leads the term of the follower's pipeline right now.</summary>
        /// <param name="status">The durable log status.</param>
        /// <param name="term">The term of the pipeline the follower's catch-up runs in.</param>
        /// <returns><see langword="null" /> when it leads; otherwise <see cref="ReplicaTransferOutcome.NotLeading" />.</returns>
        /// <remarks>Runs under the commit gate.</remarks>
        private ReplicaTransferOutcome? LeadershipRefusal(in FollowerLogStatus status, ulong term)
        {
            if (!committer.IsStarted)
                return ReplicaTransferOutcome.NotLeading;

            if (committer.Election is not { } election)
                return StaticLeaderTerm.TryResolve(in status, out var staticTerm) && staticTerm == term ? null : ReplicaTransferOutcome.NotLeading;

            var view = election.ReadRoute();
            var leads = (view.HasAuthority || view.LedWithoutAuthority) && view.Term == term && view.HighestObservedTerm <= term;
            return leads && status.CurrentTerm == term && committer.Tenure is { } tenure && tenure.Term == term ? null : ReplicaTransferOutcome.NotLeading;
        }
    }

    private static ReplicaTransferOutcome CaptureOutcome(GroupSnapshotCaptureOutcome outcome) => outcome switch
    {
        GroupSnapshotCaptureOutcome.Captured => ReplicaTransferOutcome.Transferred,
        GroupSnapshotCaptureOutcome.NotReady or GroupSnapshotCaptureOutcome.NotComplete => ReplicaTransferOutcome.NotReady,
        GroupSnapshotCaptureOutcome.NothingApplied => ReplicaTransferOutcome.NothingApplied,
        GroupSnapshotCaptureOutcome.UnresolvedOutcome => ReplicaTransferOutcome.UnresolvedOutcome,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported group snapshot capture outcome."),
    };
}
