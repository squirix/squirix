using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>The applied index flush and the owned log compaction of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterCompaction
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Persists the in-memory applied index of the owned group log once the cache journal holds every applied entry durably.</summary>
        /// <param name="durability">The node cache journal whose frames the applies appended.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when the durable applied index is at least the in-memory one read at the start.</returns>
        /// <exception cref="InvalidOperationException">The owned group log refused the applied advance.</exception>
        /// <remarks>
        /// Runs outside the commit gate. Every entry at or below the applied index read here returned from its apply, which appends its
        /// cache journal frame first, so the durability barrier awaited next covers all of them; only then does the log advance its applied
        /// index and release the applied payloads, so a crash never leaves the log claiming an apply the cache journal lost. Nothing is
        /// done while the durable applied index is already there.
        /// </remarks>
        internal async Task FlushAppliedAsync(IJournalDurabilityCoordinator durability, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(durability);
            committer.ThrowIfDisposed();
            if (!committer.Registry.TryGetLog(committer.GroupId, out var log))
                return;

            await committer.Applier.FlushAsync(log, durability, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Compacts the owned group log through its commit index once it reaches a threshold and nothing still needs its entries.</summary>
        /// <param name="policy">The compaction thresholds.</param>
        /// <param name="durability">The node cache journal whose frames the applies appended.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The step outcome; only <see cref="ReplicaLogCompactionOutcome.Compacted" /> changes the log.</returns>
        /// <remarks>
        /// The durable applied boundary is captured under the commit gate. The file is prepared outside both gates so newer writes
        /// can finish. Publication checks the same tenure and captured boundary before preserving the latest WAL tail.
        /// </remarks>
        internal async Task<ReplicaLogCompactionOutcome> CompactOwnedLogAsync(
            ReplicaLogCompactionPolicy policy,
            IJournalDurabilityCoordinator durability,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(durability);
            committer.ThrowIfDisposed();
            if (!committer.Registry.TryGetLog(committer.GroupId, out var log))
                return ReplicaLogCompactionOutcome.NotReady;

            var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
            if (!policy.IsReachedBy(in retention))
                return ReplicaLogCompactionOutcome.BelowThreshold;

            // Past the retention limit a follower that is not ready, or stayed behind the commit index while the write majority holds without
            // it, no longer holds the log: it is repaired by catch-up or a state transfer on return.
            var pastLimit = policy.IsPastRetentionLimit(in retention);
            var eligibility = committer.Registry.EligibilityFor(committer.GroupId);
            var (outcome, capture, coordinator) = await committer.CaptureWithRetriesAsync(log, eligibility, pastLimit, durability, cancellationToken).ConfigureAwait(false);
            return capture is { } plan && coordinator is { } capturedCoordinator
                ? await committer.PrepareAndFinishAsync(log, capturedCoordinator, eligibility, pastLimit, plan, cancellationToken).ConfigureAwait(false)
                : outcome;
        }

        private async Task<(ReplicaLogCompactionOutcome Outcome, GroupSnapshotCompaction? Capture, ReplicaCommitCoordinator? Coordinator)> CaptureWithRetriesAsync(
            IFollowerLog log,
            ReplicaEligibility eligibility,
            bool pastLimit,
            IJournalDurabilityCoordinator durability,
            CancellationToken cancellationToken)
        {
            // An advisory check first, without the gate: a follower that is down or lagging then refuses the step without holding the
            // gate for the follower wait. The decisive check under the gate does not wait for followers.
            using var budget = new CancellationTokenSource(committer.CompactionWaitBudget, committer.BudgetTimeProvider);
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
            const int maxAttempts = 3;
            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                AsyncLockHolder guard;
                var leaveBehind = ReplicaLogCompactionStep.NoSlots;
                try
                {
                    if (attempt > 1)
                        await Task.Delay(TimeSpan.FromMilliseconds(5), committer.Clock, waiting.Token).ConfigureAwait(false);

                    if (committer.ReadCoordinator() is { } running)
                    {
                        var observed = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
                        var (refusal, advised) = await ReplicaLogCompactionStep.AdviseFollowersAsync(running, eligibility, pastLimit, observed.CommitIndex, committer.Clock, waiting.Token).ConfigureAwait(false);
                        if (refusal is { } refused)
                            return (refused, null, null);

                        leaveBehind = advised;
                    }

                    guard = await committer.Gate.LockAsync(waiting.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    return (ReplicaLogCompactionOutcome.Busy, null, null);
                }

                using (guard)
                {
                    committer.ThrowIfDisposed();
                    var result = await committer.CaptureHeldAsync(log, (eligibility, pastLimit, leaveBehind), durability, cancellationToken).ConfigureAwait(false);
                    if (result.Outcome != ReplicaLogCompactionOutcome.FollowerBehind)
                        return result;
                }
            }

            return (ReplicaLogCompactionOutcome.FollowerBehind, null, null);
        }

        private async Task<ReplicaLogCompactionOutcome> PrepareAndFinishAsync(
            IFollowerLog log,
            ReplicaCommitCoordinator coordinator,
            ReplicaEligibility eligibility,
            bool pastLimit,
            GroupSnapshotCompaction capture,
            CancellationToken cancellationToken)
        {
            GroupSnapshotCheckpointStore.PreparedSnapshot? prepared;
            try
            {
                prepared = await log.PrepareCompactionAsync(capture, cancellationToken).ConfigureAwait(false);
            }
            catch (GroupSnapshotTooLargeException)
            {
                return ReplicaLogCompactionOutcome.SnapshotTooLarge;
            }

            using var generation = prepared;
            if (generation == null)
                return ReplicaLogCompactionOutcome.NotReady;

            using var budget = new CancellationTokenSource(committer.CompactionWaitBudget, committer.BudgetTimeProvider);
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
            AsyncLockHolder guard;
            try
            {
                guard = await committer.Gate.LockAsync(waiting.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return ReplicaLogCompactionOutcome.Busy;
            }

            using (guard)
            {
                committer.ThrowIfDisposed();
                var canPublish = committer.IsStarted && ReferenceEquals(coordinator, committer.Coordinator) &&
                                 (committer.Election == null || committer.Tenure is { Authorized: true });
                return (committer.TransferPin.IsHeld, canPublish) switch
                {
                    (true, _) => ReplicaLogCompactionOutcome.FollowerNotReady,
                    (false, true) => await ReplicaLogCompactionStep.FinishAsync(log, coordinator, eligibility, pastLimit, capture, generation, cancellationToken).ConfigureAwait(false),
                    _ => ReplicaLogCompactionOutcome.NotReady,
                };
            }
        }

        /// <summary>Captures the compaction boundary once the commit gate is held and nothing keeps the log.</summary>
        /// <param name="log">The owned group log.</param>
        /// <param name="slots">The participation state of the owned group slots, whether the log reached its retention limit, and the ready follower slots that may be left behind.</param>
        /// <param name="durability">The node cache journal whose frames the applies appended.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The step outcome; a log pinned by a state transfer is skipped like a follower that is not ready, past the retention limit too.</returns>
        /// <remarks>
        /// A state transfer pins the log from its capture to its end, so every entry above the transferred index and the term at it stay.
        /// An elected leader compacts only once its leader-term entry is committed: the authority check reads the term of that entry.
        /// </remarks>
        private async Task<(ReplicaLogCompactionOutcome Outcome, GroupSnapshotCompaction? Capture, ReplicaCommitCoordinator? Coordinator)> CaptureHeldAsync(
            IFollowerLog log,
            (ReplicaEligibility Eligibility, bool PastLimit, int LeaveBehind) slots,
            IJournalDurabilityCoordinator durability,
            CancellationToken cancellationToken)
        {
            if (committer.TransferPin.IsHeld)
                return (ReplicaLogCompactionOutcome.FollowerNotReady, null, null);
            if (!committer.IsStarted || committer.Coordinator is not { } coordinator ||
                (committer.Election != null && committer.Tenure is not { Authorized: true }))
                return (ReplicaLogCompactionOutcome.NotReady, null, null);

            GroupSnapshotCompaction? capture = null;
            var outcome = await ReplicaLogCompactionStep.RunAsync(
                log, coordinator, slots, committer.Applier.AppliedIndex, durability, CaptureAsync, cancellationToken).ConfigureAwait(false);
            return (outcome, capture, coordinator);

            async Task<GroupCompactionOutcome> CaptureAsync(ulong index, CancellationToken token)
            {
                var (captured, boundary) = await log.CaptureCompactionAsync(index, token).ConfigureAwait(false);
                capture = boundary;
                return captured;
            }
        }
    }
}
