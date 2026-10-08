using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The commits under the commit gate, the commit attempt, and the coordinator construction of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterCommits
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Commits one write under the commit gate: prepares it at the next log index, commits it, and decodes its outcome.</summary>
        /// <typeparam name="TState">The arguments of the write.</typeparam>
        /// <typeparam name="TResult">The decoded outcome.</typeparam>
        /// <param name="write">The cache scope and the client operation identifier of the write.</param>
        /// <param name="state">The arguments of the write, handed to <paramref name="prepare" />.</param>
        /// <param name="fingerprint">
        /// Computes the operation fingerprint of the write from its arguments; called only when the write is refused before it is
        /// prepared and an entry with its identity is retained.
        /// </param>
        /// <param name="prepare">Prepares the mutation from the running factory, the arguments, and the log index it is appended at.</param>
        /// <param name="decode">Decodes the committed outcome.</param>
        /// <param name="cancellationToken">Cancellation token for queueing only; the commit itself is budget-bounded.</param>
        /// <returns>The decoded outcome of the committed write.</returns>
        /// <exception cref="ServerOpIdMismatchException">The operation identifier is reused with another request.</exception>
        /// <exception cref="SquirixException">The outcome of the operation is unknown, or the write is refused retryably.</exception>
        /// <remarks>The typed writes built on this method are in <see cref="ReplicaGroupCommitterWrites" />.</remarks>
        internal async Task<TResult> CommitAsync<TState, TResult>(
            (string Scope, string OperationId) write,
            TState state,
            Func<TState, byte[]> fingerprint,
            Func<ReplicaMutationFactory, TState, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> prepare,
            Func<ReadOnlyMemory<byte>, ValueTask<TResult>> decode,
            CancellationToken cancellationToken)
        {
            committer.ThrowIfDisposed();
            await committer.WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            using var guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            var starting = committer.EnsureStartedAsync(true, cancellationToken);
            if (await starting.CaptureFailureAsync().ConfigureAwait(false) != null)
            {
                // Whatever refused the start, a retained entry of this operation decides the answer. A retry of a committed operation replays
                // its outcome: it needs no majority and no apply. A retry of an operation whose entry is appended but not yet committed
                // (possibly by the process before a restart) must neither re-execute nor be told it failed: its outcome stays unknown until a
                // commit resolves the entry. The same identifier with another request is a reuse, whatever the state of the entry. Without a
                // retained entry the refusal stands and is rethrown by the await below.
                var retained = LookupRetained(committer.Registry.TryGetLog(committer.GroupId, out var log) ? log : null, write, state, fingerprint, out var recorded);
                if (retained == GroupIdempotencyLookup.Found)
                    return await decode(recorded.OutcomePayload).ConfigureAwait(false);
                if (retained == GroupIdempotencyLookup.Mismatch)
                    throw new ServerOpIdMismatchException();
                if (retained == GroupIdempotencyLookup.Unresolved)
                    throw ServerOpContract.CommitOutcomeUnknown();
            }

            var (coordinator, factory) = await starting.ConfigureAwait(false);
            var index = committer.PeekNextIndex();
            var mutation = await prepare(factory, state, index, cancellationToken).ConfigureAwait(false);
            var outcome = await committer.CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false);
            return await decode(outcome).ConfigureAwait(false);
        }

        /// <summary>Runs one expiry of a key under the commit gate: decides it on the leader clock and commits the tombstone of an expired entry.</summary>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <returns><see langword="null" /> once the tombstone is committed, or when the key is absent; the stored entry when it is live.</returns>
        /// <remarks>
        /// One run serves every caller of the key, so it takes no caller token: the commit budget bounds the wait before the append, and the
        /// commit itself is budget-bounded.
        /// </remarks>
        internal async Task<NodeCacheEntry<object?>?> CommitExpiryAsync(string cacheName, string key)
        {
            committer.ThrowIfDisposed();
            using var budget = new CancellationTokenSource(committer.CommitBudget, committer.BudgetTimeProvider);
            await committer.WaitForLocalRecoveryAsync(budget.Token).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            using var guard = await committer.Gate.LockAsync(budget.Token).ConfigureAwait(false);
            var (coordinator, factory) = await committer.EnsureStartedAsync(true, budget.Token).ConfigureAwait(false);
            var (tombstone, current) = await factory.PrepareExpireAsync(cacheName, key, committer.PeekNextIndex(), budget.Token).ConfigureAwait(false);
            if (tombstone == null)
                return current;

            _ = await committer.CommitWithPreAppendResyncAsync(coordinator, tombstone).ConfigureAwait(false);
            return null;
        }

        /// <summary>Closes the senders of a pipeline and logs a failure instead of throwing it, so the teardown that follows always runs.</summary>
        /// <param name="pipeline">The pipeline to close.</param>
        /// <returns>An asynchronous operation.</returns>
        internal async ValueTask CloseSendersAsync(ReplicaGroupCommitPipeline pipeline)
        {
            if (await pipeline.CloseAsync().ConfigureAwait(false) is { } failure)
                ServerLog.ReplicaFollowerSenderCloseFailed(committer.Log, failure);
        }

        /// <summary>Creates the commit coordinator of a start over its pipeline and the recovered leader tail.</summary>
        /// <param name="slots">The number of replicas of the group, this node included, and the slot of this node, the leader.</param>
        /// <param name="pipeline">The pipeline of the new coordinator.</param>
        /// <param name="log">The owned group log.</param>
        /// <param name="status">Durable log status of the leader.</param>
        /// <param name="eligibility">Replica eligibility of the group.</param>
        /// <param name="recoveredTail">The recovered uncommitted tail entries.</param>
        /// <returns>The coordinator.</returns>
        internal ReplicaCommitCoordinator CreateCoordinator(
            (int ReplicaCount, int LeaderReplicaIndex) slots,
            IReplicaCommitPipeline pipeline,
            IFollowerLog log,
            in FollowerLogStatus status,
            ReplicaEligibility eligibility,
            ReplicaRecoveredTail? recoveredTail)
        {
            var logger = committer.Log;
            return new ReplicaCommitCoordinator(
                new ReplicaCommitCoordinatorOptions(slots.ReplicaCount, status.LastLogIndex, status.CommitIndex, ReplicaGroupCommitter.MaxInFlight, slots.LeaderReplicaIndex),
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

        /// <summary>Commits a prepared mutation on the coordinator and maps every failure to the stable client contract.</summary>
        /// <param name="coordinator">The running coordinator.</param>
        /// <param name="mutation">The prepared mutation.</param>
        /// <returns>The committed outcome payload.</returns>
        /// <exception cref="SquirixException">The outcome is unknown, or the write is refused retryably.</exception>
        /// <exception cref="ServerOpIdMismatchException">The operation identifier is reused with another request.</exception>
        /// <exception cref="Grpc.Core.RpcException">The log refused the local append with a stale term: stale-term, nothing was written.</exception>
        /// <remarks>Runs under the commit gate.</remarks>
        private async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation)
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

                // The log refused the entry because a higher term reached it durably: nothing was appended, and this leadership is stale.
                if (error is ReplicaTermSupersededException)
                    throw StaleTermFailure.Create(null, 0);
                throw;
            }
        }

        /// <summary>Tells whether the owned group log holds the entry of a prepared mutation.</summary>
        /// <param name="mutation">The prepared mutation.</param>
        /// <returns><see langword="true" /> when the log holds the entry.</returns>
        private ValueTask<bool> HoldsEntryAsync(PreparedReplicaMutation mutation) => committer.Registry.HoldsEntryAsync(committer.GroupId, mutation);
    }

    /// <summary>Looks up the entry retained in the owned group log for the identity of a write.</summary>
    /// <typeparam name="TState">The type of the write arguments.</typeparam>
    /// <param name="log">The owned group log, or <see langword="null" /> when it is not open.</param>
    /// <param name="write">The cache scope and the client operation identifier of the write.</param>
    /// <param name="state">The arguments of the write.</param>
    /// <param name="fingerprint">Computes the operation fingerprint of the write.</param>
    /// <param name="record">The retained record when the lookup finds the outcome; otherwise <see langword="default" />.</param>
    /// <returns>The lookup; a miss when the owned group log is not open.</returns>
    /// <remarks>The fingerprint, which encodes and hashes the request, is computed only once an entry with the identity is found.</remarks>
    private static GroupIdempotencyLookup LookupRetained<TState>(
        IFollowerLog? log,
        (string Scope, string OperationId) write,
        TState state,
        Func<TState, byte[]> fingerprint,
        out GroupIdempotencyRecord record)
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        record = default;
        return log == null || log.Idempotency.Lookup(write.Scope, write.OperationId, [], out _) == GroupIdempotencyLookup.Miss
            ? GroupIdempotencyLookup.Miss
            : log.Idempotency.Lookup(write.Scope, write.OperationId, fingerprint(state), out record);
    }

    private static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
}
