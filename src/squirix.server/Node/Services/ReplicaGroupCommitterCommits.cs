using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The commit attempt and the coordinator construction of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterCommits
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Commits a prepared mutation on the coordinator and maps every failure to the stable client contract.</summary>
        /// <param name="coordinator">The running coordinator.</param>
        /// <param name="mutation">The prepared mutation.</param>
        /// <returns>The committed outcome payload.</returns>
        /// <exception cref="SquirixException">The outcome is unknown, or the write is refused retryably.</exception>
        /// <exception cref="ServerOpIdMismatchException">The operation identifier is reused with another request.</exception>
        /// <remarks>Runs under the commit gate.</remarks>
        internal async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation)
        {
            try
            {
                return await coordinator.CommitAsync(mutation, committer.CommitBudget, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception error) when (IsPostAppendOutcome(error))
            {
                // A durable majority may hold the entry: keep the reservation and sequencing untouched and report the stable contract
                // (gRPC Unavailable with COMMIT_OUTCOME_UNKNOWN), so callers stop instead of retrying under a new identity.
                ServerLog.ReplicaCommitOutcomeUnknown(committer.Log, error);
                throw ServerOpContract.CommitOutcomeUnknown();
            }
            catch (InvalidOperationException error) when (error.Message.StartsWith(ReplicaCommitCoordinator.IdempotencyCapacityCode, StringComparison.Ordinal))
            {
                // Refused when the identity was reserved, before anything was appended: the pipeline positions stand, so the started state
                // is kept, and the caller gets a retryable refusal while older outcomes age out.
                throw ServerOpContract.TooManyRequests(ReplicaCommitCoordinator.IdempotencyCapacityCode);
            }
            catch (InvalidOperationException error) when (error.Message.StartsWith(ReplicaCommitCoordinator.FingerprintMismatchCode, StringComparison.Ordinal))
            {
                // Refused the same way at the lookup, as a reuse of the identifier.
                throw new ServerOpIdMismatchException();
            }
            catch (Exception error)
            {
                // The local appending was refused before anything was marked appended: an interrupted
                // append may leave the durable log ahead of the pipeline positions, so drop the started
                // state and rebuild from status.LastLogIndex on the next attempt.
                committer.DropStartedState();

                // The log may still hold the entry (its frames were durable when the write behind them failed): the next start recovers and
                // pins it as the tail, and a majority may commit it, so the caller must not be told the write was refused.
                if (await committer.HoldsEntryAsync(mutation).ConfigureAwait(false))
                {
                    ServerLog.ReplicaCommitOutcomeUnknown(committer.Log, error);
                    throw ServerOpContract.CommitOutcomeUnknown();
                }

                // The commit runs on the budget only, so a cancellation here is the budget expiring before the append: a definite refusal.
                if (error is OperationCanceledException)
                    throw ServerOpContract.TooManyRequests(ReplicaGroupCommitter.CommitBudgetRefusalReason);
                throw;
            }
        }

        /// <summary>Creates the commit coordinator of a start over its pipeline and the recovered leader tail.</summary>
        /// <param name="replicaCount">The number of replicas of the group, this node included.</param>
        /// <param name="pipeline">The pipeline of the new coordinator.</param>
        /// <param name="log">The owned group log.</param>
        /// <param name="status">Durable log status of the leader.</param>
        /// <param name="eligibility">Replica eligibility of the group.</param>
        /// <param name="recoveredTail">The recovered uncommitted tail entries.</param>
        /// <returns>The coordinator.</returns>
        internal ReplicaCommitCoordinator CreateCoordinator(
            int replicaCount,
            IReplicaCommitPipeline pipeline,
            IFollowerLog log,
            in FollowerLogStatus status,
            ReplicaEligibility eligibility,
            ReplicaRecoveredTail? recoveredTail)
        {
            var logger = committer.Log;
            return new ReplicaCommitCoordinator(
                new ReplicaCommitCoordinatorOptions(replicaCount, status.LastLogIndex, status.CommitIndex, ReplicaGroupCommitter.MaxInFlight),
                pipeline,
                ReplicaGroupCommitter.NoOpCommitHooks.Instance,
                log.Idempotency,
                eligibility,
                recoveredTail)
            {
                // The coordinator's teardown is part of this committer's dispose, so it never waits longer than this committer's budget.
                ShutdownBudget = committer.ShutdownBudget < ReplicaCommitCoordinator.DefaultShutdownBudget ? committer.ShutdownBudget : ReplicaCommitCoordinator.DefaultShutdownBudget,
                BudgetTimeProvider = committer.BudgetTimeProvider,
                ShutdownLeakReporter = budget => ServerLog.ReplicaCoordinatorLeakedOnShutdown(logger, budget),
                AbandonedWorkFaultReporter = error => ServerLog.ReplicaCoordinatorAbandonedWorkFaulted(logger, error),
            };
        }
    }

    private static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
}
