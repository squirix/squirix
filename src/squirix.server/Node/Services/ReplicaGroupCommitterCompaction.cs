using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
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
        /// The thresholds, and advisorily the followers, are checked without the commit gate. Everything else is one step under it: no
        /// write can append, commit, or apply between the checks and the compaction, so a steady write load cannot keep moving the commit
        /// index past the applied one. A write arriving meanwhile waits for the step and then appends after the compacted log.
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

            // An advisory check first, without the gate: a follower that is down or lagging then refuses the step without holding the
            // gate for the whole follower wait on every pass. The decisive check runs again under the gate. The wait budget bounds only these
            // waits: once the gate is held, the compaction runs to its end, so a budget never cancels a durable rewrite of the log.
            var eligibility = committer.Registry.EligibilityFor(committer.GroupId);
            using var budget = new CancellationTokenSource(committer.CompactionWaitBudget, committer.BudgetTimeProvider);
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
            AsyncLockHolder guard;
            try
            {
                if (committer.ReadCoordinator() is { } running)
                {
                    var observed = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
                    if (await ReplicaLogCompactionStep.AwaitFollowersAsync(running, eligibility, observed.CommitIndex, committer.Clock, waiting.Token).ConfigureAwait(false) is { } refused)
                        return refused;
                }

                guard = await committer.Gate.LockAsync(waiting.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                return ReplicaLogCompactionOutcome.Busy;
            }

            using var held = guard;
            committer.ThrowIfDisposed();

            // An elected leader compacts only once its leader-term entry is committed: the authority check reads the term of that entry.
            return committer.IsStarted && committer.Coordinator is { } coordinator && (committer.Election == null || committer.Tenure is { Authorized: true })
                ? await ReplicaLogCompactionStep.RunAsync(log, coordinator, eligibility, committer.Applier.AppliedIndex, durability, committer.Clock, cancellationToken).ConfigureAwait(false)
                : ReplicaLogCompactionOutcome.NotReady;
        }
    }
}
