using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The commits under the commit gate, the commit attempt, and the coordinator construction of <see cref="ReplicaGroupCommitter" />.</summary>
internal static class ReplicaGroupCommitterCommits
{
    /// <summary>The most keys one expiry batch decides, so one batch fits the acknowledgement buffer of a follower slot and the backlog of a sender.</summary>
    internal const int MaxExpiriesPerCommit = 64;

    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Commits one write: queues it for a commit batch of the group, prepares it at its log index under the commit gate, commits it, and decodes its outcome.</summary>
        /// <typeparam name="TState">The arguments of the write.</typeparam>
        /// <typeparam name="TResult">The decoded outcome.</typeparam>
        /// <param name="write">The cache scope, the client operation identifier, and the cache key of the write.</param>
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
        /// <exception cref="Grpc.Core.RpcException">
        /// The write was refused before anything of it was scheduled: no leader (shutdown, or a retired leadership), no verified majority, or a stale term; nothing was written.
        /// </exception>
        /// <remarks>
        /// The typed writes built on this method are in <see cref="ReplicaGroupCommitterWrites" />, the batching in <see cref="ReplicaGroupCommitterBatches" />.
        /// The prepare runs on the commit budget, not on <paramref name="cancellationToken" />: a write a leader took into a batch is decided whatever its caller does.
        /// </remarks>
        internal async Task<TResult> CommitAsync<TState, TResult>(
            (string Scope, string OperationId, string Key) write,
            TState state,
            Func<TState, byte[]> fingerprint,
            Func<ReplicaMutationFactory, TState, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> prepare,
            Func<ReadOnlyMemory<byte>, ValueTask<TResult>> decode,
            CancellationToken cancellationToken)
        {
            var request = new ReplicaStateCommitRequest<TState>(write, state, fingerprint, prepare);
            try
            {
                committer.ThrowIfDisposed();
                await committer.WaitForLocalRecoveryAsync(cancellationToken).ConfigureAwait(false);
                committer.ThrowIfDisposed();
            }
            catch (ObjectDisposedException)
            {
                // The committer is disposing: the write was never admitted, so nothing of it was read or written. A retained entry of this operation
                // still decides the answer first.
                return await decode(committer.Refused(request)).ConfigureAwait(false);
            }

            return await decode(await committer.SubmitAsync(request, cancellationToken).ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <summary>Runs the expiry of several keys under one hold of the commit gate: decides each on the leader clock and commits the tombstones of the expired entries as one run.</summary>
        /// <param name="keys">Target caches and keys, at most <see cref="MaxExpiriesPerCommit" />; a repeated key is decided once.</param>
        /// <returns>
        /// One result per input key, in order. A live key is <see cref="ReplicaExpiryStatus.Live" /> with its stored entry, an absent key is
        /// <see cref="ReplicaExpiryStatus.Absent" />, and an expired key is <see cref="ReplicaExpiryStatus.Tombstoned" /> once its tombstone is
        /// committed. When the run of tombstones fails, each key it carried, or whose tombstone is in an unresolved state, is
        /// <see cref="ReplicaExpiryStatus.Failed" /> with the client failure; the other keys keep their decided result.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="keys" /> holds more than <see cref="MaxExpiriesPerCommit" /> keys.</exception>
        /// <exception cref="Grpc.Core.RpcException">The batch was refused before any key was decided: no leader or a stale term.</exception>
        /// <remarks>
        /// Each key is read again under the gate, so a key rewritten or touched after the scan of the caller is returned live and gets no tombstone,
        /// and the tombstones take consecutive log indexes. They are committed with one append and one majority; a failure of that run is the same
        /// for every tombstone of it. A tombstone whose identity the group already retains is not appended again: a committed one is reported as
        /// <see cref="ReplicaExpiryStatus.Tombstoned" />, an unresolved one as <see cref="ReplicaExpiryStatus.Failed" /> with an unknown outcome, so
        /// a run of one tombstone and a longer run map the same. A batch that finds nothing to tombstone appends nothing. A key whose
        /// decision fails for a reason other than the budget is <see cref="ReplicaExpiryStatus.Failed" /> alone. It takes no caller token, since one
        /// batch serves every caller of its keys: the commit budget bounds the wait before the append.
        /// The request of the whole run is built on the committer thread under the gate; the cost of that is measured with the SetAsync latency.
        /// </remarks>
        internal async Task<ReplicaExpiryResult[]> CommitExpiriesAsync(IReadOnlyList<(string CacheName, string Key)> keys)
        {
            ArgumentNullException.ThrowIfNull(keys);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(keys.Count, MaxExpiriesPerCommit);
            committer.ThrowIfDisposed();
            var batch = new ExpiryRun(keys.Count);
            if (keys.Count == 0)
                return batch.Results;

            using var budget = new CancellationTokenSource(committer.CommitBudget, committer.BudgetTimeProvider);
            await committer.WaitForLocalRecoveryAsync(budget.Token).ConfigureAwait(false);
            committer.ThrowIfDisposed();
            using var guard = await committer.Gate.LockAsync(budget.Token).ConfigureAwait(false);
            var (coordinator, factory) = await committer.EnsureStartedAsync(true, budget.Token).ConfigureAwait(false);
            await committer.DecideExpiriesAsync(factory, keys, batch, budget.Token).ConfigureAwait(false);
            if (batch.Run.Count > 0)
            {
                try
                {
                    var run = batch.Run;
                    _ = await committer.CommitWithPreAppendResyncAsync(coordinator, run[0], run.Count > 1 ? [.. run] : null).ConfigureAwait(false);
                }
                catch (Exception error) when (error is SquirixException or RpcException or ServerOpIdMismatchException)
                {
                    batch.FailRun(error);
                }
            }

            batch.CopyRepeats();
            return batch.Results;
        }

        /// <summary>Runs the expiry of a chunk of keys for the expiry queue: the entry or the failure of each key.</summary>
        /// <param name="keys">Target caches and keys, at most <see cref="MaxExpiriesPerCommit" />.</param>
        /// <returns>One pair per key, in order: the live entry, or the failure of that key's tombstone; both <see langword="null" /> once the tombstone is committed or when the key is absent.</returns>
        /// <exception cref="SquirixException">The chunk was refused as a whole.</exception>
        internal async Task<(NodeCacheEntry<object?>? Entry, Exception? Failure)[]> CommitExpiryChunkAsync(IReadOnlyList<(string CacheName, string Key)> keys)
        {
            var results = await committer.CommitExpiriesAsync(keys).ConfigureAwait(false);
            var mapped = new (NodeCacheEntry<object?>? Entry, Exception? Failure)[results.Length];
            for (var i = 0; i < results.Length; i++)
                mapped[i] = (results[i].Entry, results[i].Failure);

            return mapped;
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
                ObserveTimeProvider = TimeProvider.System,
                ShutdownLeakReporter = budget => ServerLog.ReplicaCoordinatorLeakedOnShutdown(logger, budget),
                AbandonedWorkFaultReporter = error => ServerLog.ReplicaCoordinatorAbandonedWorkFaulted(logger, error),
            };
        }

        /// <summary>Commits a prepared mutation, or a dense run that starts with it, on the coordinator and maps every failure to the stable client contract.</summary>
        /// <param name="coordinator">The running coordinator.</param>
        /// <param name="mutation">The prepared mutation; the first entry of <paramref name="run" /> when there is one.</param>
        /// <param name="run">The prepared mutations of a run of several entries, consecutive from the next log index; <see langword="null" /> for a single write.</param>
        /// <returns>The committed outcome payload; of the last entry of a run.</returns>
        /// <exception cref="SquirixException">The outcome is unknown, or the write is refused retryably.</exception>
        /// <exception cref="ServerOpIdMismatchException">The operation identifier is reused with another request.</exception>
        /// <exception cref="Grpc.Core.RpcException">The log refused the local append with a stale term: stale-term, nothing was written.</exception>
        /// <remarks>
        /// Runs under the commit gate. A top-level <see cref="ReplicaShutdownRefusedException" /> that finds no entry of the write in the log was raised before
        /// any entry of it was scheduled, so it is answered as no leader; an inner exception or a plain disposal is never mapped. A failure of a run
        /// is reported for every entry of it. The log is asked whether it holds the first entry only: a run is appended as one request, so a durable
        /// prefix always holds it.
        /// </remarks>
        internal async ValueTask<ReadOnlyMemory<byte>> CommitWithPreAppendResyncAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation mutation, PreparedReplicaMutation[]? run = null)
        {
            var failedBefore = committer.LocalAppends.Snapshot().Failed;
            try
            {
                if (run == null)
                    return await coordinator.CommitAsync(mutation, committer.CommitBudget, CancellationToken.None).ConfigureAwait(false);

                await coordinator.CommitBatchAsync(run, committer.CommitBudget, CancellationToken.None).ConfigureAwait(false);
                return run[^1].OutcomePayload;
            }
            catch (Exception error) when (IsPostAppendOutcome(error))
            {
                if (error.InnerException is GroupLogAppendUnconfirmedException unconfirmed)
                {
                    // The own append failed with its frames possibly on disk: the interrupted append may leave the durable log ahead of the
                    // pipeline positions, so rebuild them on the next start, and report the fault of the log like any refused local append.
                    committer.DropStartedState();
                    committer.ReportLocalAppendFault(unconfirmed, failedBefore);
                }

                // A durable majority may hold the entry: keep the reservation and sequencing untouched and report the stable contract
                // (gRPC Unavailable with COMMIT_OUTCOME_UNKNOWN), so callers stop instead of retrying under a new identity.
                ServerLog.ReplicaCommitOutcomeUnknown(committer.Log, error);
                committer.Metrics?.ReportUnknownCommit(GroupRecordKinds.FromScope(mutation.OperationScope));
                throw ServerOpContract.CommitOutcomeUnknown();
            }
            catch (Exception error)
            {
                if (await committer.MapPreAppendFailureAsync(error, mutation, failedBefore).ConfigureAwait(false) is { } mapped)
                    throw mapped;

                throw;
            }
        }

        /// <summary>Maps a failure of the local appending of a run to the stable client contract; it returns <see langword="null" /> for a failure that has no mapping.</summary>
        /// <param name="error">The failure of the commit.</param>
        /// <param name="mutation">The first entry of the run.</param>
        /// <param name="failedBefore">The number of failed local appends before the commit.</param>
        /// <param name="reportUnknown">Whether an unknown outcome is counted here; a run counts it once per write of it, where each write is decided.</param>
        /// <returns>The client failure, or <see langword="null" /> when <paramref name="error" /> is to be rethrown as it is.</returns>
        /// <remarks>The log may hold the entry, so its outcome is unknown, or the budget ended before the append; the leadership is stale, or there is no leader.</remarks>
        internal async ValueTask<Exception?> MapPreAppendFailureAsync(Exception error, PreparedReplicaMutation mutation, ulong failedBefore, bool reportUnknown = true)
        {
            // Refused when the identities were reserved, before anything was appended: the pipeline positions stand, so the started
            // state is kept.
            if (error is InvalidOperationException invalid && DefiniteRefusal(invalid) is { } refusal)
            {
                // One report per refused call, whether it carried one entry or a run.
                if (invalid.Message.StartsWith(ReplicaCommitCoordinator.IdempotencyCapacityCode, StringComparison.Ordinal))
                    committer.Metrics?.ReportIdempotencyRefusal(committer.Probe.SelfId, committer.GroupId);

                return refusal;
            }

            // The local appending was refused before anything was marked appended: an interrupted
            // append may leave the durable log ahead of the pipeline positions, so drop the started
            // state and rebuild from status.LastLogIndex on the next attempt.
            committer.DropStartedState();
            committer.ReportLocalAppendFault(error, failedBefore);

            // The log may still hold the entry (its frames were durable when the write behind them failed): the next start recovers and
            // pins it as the tail, and a majority may commit it, so the caller must not be told the write was refused.
            if (await committer.HoldsEntryAsync(mutation).ConfigureAwait(false))
            {
                ServerLog.ReplicaCommitOutcomeUnknown(committer.Log, error);
                if (reportUnknown)
                    committer.Metrics?.ReportUnknownCommit(GroupRecordKinds.FromScope(mutation.OperationScope));

                return ServerOpContract.CommitOutcomeUnknown();
            }

            return error switch
            {
                // The commit runs on the budget only, so a cancellation here is the budget expiring before the append: a definite refusal.
                OperationCanceledException => ServerOpContract.TooManyRequests(ReplicaGroupCommitter.CommitBudgetRefusalReason),

                // The log refused the entry because a higher term reached it durably: nothing was appended, and this leadership is stale.
                ReplicaTermSupersededException => StaleTermFailure.Create(null, 0),
                ReplicaShutdownRefusedException => ServerOpContract.NoLeaderAuthority(),
                _ => null,
            };
        }

        /// <summary>Decides each distinct key of a batch under the gate and collects the tombstones of the expired entries that are new to the group.</summary>
        /// <param name="factory">The running mutation factory.</param>
        /// <param name="keys">The keys of the batch.</param>
        /// <param name="batch">Receives the result of each key and the tombstones of the run.</param>
        /// <param name="cancellationToken">The budget token.</param>
        /// <returns>An asynchronous operation.</returns>
        /// <remarks>
        /// The tombstones take consecutive indexes from the next log index, so a tombstone the group already retains is left out before the
        /// next key is prepared: a committed one is answered as tombstoned, an unresolved one fails alone with an unknown outcome. A key whose
        /// decision throws fails alone, unless the budget ended.
        /// </remarks>
        private async Task DecideExpiriesAsync(ReplicaMutationFactory factory, IReadOnlyList<(string CacheName, string Key)> keys, ExpiryRun batch, CancellationToken cancellationToken)
        {
            var firstOf = new Dictionary<(string CacheName, string Key), int>(keys.Count);
            var firstIndex = committer.PeekNextIndex();
            for (var i = 0; i < keys.Count; i++)
            {
                if (!firstOf.TryAdd(keys[i], i))
                {
                    batch.Repeats[i] = firstOf[keys[i]];
                    continue;
                }

                PreparedReplicaMutation? tombstone;
                NodeCacheEntry<object?>? current;
                try
                {
                    (tombstone, current) = await factory.PrepareExpireAsync(keys[i].CacheName, keys[i].Key, firstIndex + ulong.CreateChecked(batch.Run.Count), cancellationToken).ConfigureAwait(false);
                }
                catch (Exception error) when (!cancellationToken.IsCancellationRequested)
                {
                    // Nothing of this key reached the run, so it fails alone; the budget ending refuses the whole batch instead.
                    batch.Results[i] = ReplicaExpiryResult.Failed(error);
                    continue;
                }

                if (tombstone == null)
                {
                    batch.Results[i] = current == null ? ReplicaExpiryResult.Absent : ReplicaExpiryResult.Live(current);
                    continue;
                }

                var retained = committer.Registry.TryGetLog(committer.GroupId, out var log)
                    ? log.Idempotency.Lookup(tombstone.OperationScope, tombstone.OperationId, tombstone.OperationFingerprint.Span, out _)
                    : GroupIdempotencyLookup.Miss;
                if (retained == GroupIdempotencyLookup.Miss)
                {
                    batch.Add(i, tombstone);
                    continue;
                }

                batch.Results[i] = RetainedExpiry(retained);
            }
        }

        /// <summary>Tells whether the owned group log holds the entry of a prepared mutation.</summary>
        /// <param name="mutation">The prepared mutation.</param>
        /// <returns><see langword="true" /> when the log holds the entry.</returns>
        private ValueTask<bool> HoldsEntryAsync(PreparedReplicaMutation mutation) => committer.Registry.HoldsEntryAsync(committer.GroupId, mutation);
    }

    /// <summary>Tells whether a failure of the coordinator is an unknown outcome after the local append.</summary>
    /// <param name="error">The failure of the commit.</param>
    /// <returns><see langword="true" /> when the outcome is unknown.</returns>
    internal static bool IsPostAppendOutcome(Exception error) =>
        error is InvalidOperationException && error.Message.StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

    /// <summary>Maps a refusal of the coordinator that happened when the identities of a run were reserved, before anything was appended.</summary>
    /// <param name="error">The failure of the commit.</param>
    /// <returns>The client failure, or <see langword="null" /> when <paramref name="error" /> is not such a refusal.</returns>
    private static Exception? DefiniteRefusal(InvalidOperationException error)
    {
        if (error.Message.StartsWith(ReplicaCommitCoordinator.IdempotencyCapacityCode, StringComparison.Ordinal))
            return ServerOpContract.TooManyRequests(ReplicaCommitCoordinator.IdempotencyCapacityCode);

        // Refused at the lookup, as a reuse of the identifier.
        if (error.Message.StartsWith(ReplicaCommitCoordinator.FingerprintMismatchCode, StringComparison.Ordinal))
            return new ServerOpIdMismatchException();

        // An entry of the run has the identity of an operation the group already knows; the run is decided again from the live entries on the next attempt.
        return error.Message.StartsWith(ReplicaCommitCoordinator.BatchIdentityKnownCode, StringComparison.Ordinal)
            ? ServerOpContract.TooManyRequests(ReplicaCommitCoordinator.BatchIdentityKnownCode)
            : null;
    }

    /// <summary>Maps a tombstone whose identity the group already retains to its result.</summary>
    /// <param name="lookup">The lookup of the identity, not a miss.</param>
    /// <returns>Tombstoned for a committed operation, otherwise the failure that names why the tombstone is not appended again.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="lookup" /> is not a named lookup result.</exception>
    private static ReplicaExpiryResult RetainedExpiry(GroupIdempotencyLookup lookup) => lookup switch
    {
        GroupIdempotencyLookup.Found => ReplicaExpiryResult.Tombstoned,
        GroupIdempotencyLookup.Mismatch => ReplicaExpiryResult.Failed(new ServerOpIdMismatchException()),
        GroupIdempotencyLookup.Unresolved => ReplicaExpiryResult.Failed(ServerOpContract.CommitOutcomeUnknown()),
        _ => throw new ArgumentOutOfRangeException(nameof(lookup), lookup, "Unsupported lookup result."),
    };

    /// <summary>The results of a batch expiry and the run of tombstones they depend on.</summary>
    [Mutable]
    private sealed class ExpiryRun
    {
        private readonly List<int> _positions;

        internal ExpiryRun(int count)
        {
            Results = new ReplicaExpiryResult[count];
            Repeats = new int[count];
            Array.Fill(Repeats, -1);
            Run = [];
            _positions = [];
        }

        /// <summary>Gets the result of each input key.</summary>
        internal ReplicaExpiryResult[] Results { get; }

        /// <summary>Gets, for each input key, the position of the first occurrence of a repeated key; minus one for a first occurrence.</summary>
        internal int[] Repeats { get; }

        /// <summary>Gets the tombstones of the run, consecutive from the next log index.</summary>
        internal List<PreparedReplicaMutation> Run { get; }

        /// <summary>Adds the tombstone of the key at a position to the run.</summary>
        /// <param name="position">The position of the key.</param>
        /// <param name="tombstone">The tombstone.</param>
        internal void Add(int position, PreparedReplicaMutation tombstone)
        {
            Run.Add(tombstone);
            _positions.Add(position);
            Results[position] = ReplicaExpiryResult.Tombstoned;
        }

        /// <summary>Fails every key of the run.</summary>
        /// <param name="failure">The failure of the run.</param>
        internal void FailRun(Exception failure)
        {
            for (var i = 0; i < _positions.Count; i++)
                Results[_positions[i]] = ReplicaExpiryResult.Failed(failure);
        }

        /// <summary>Gives each repeated key the result of its first occurrence.</summary>
        internal void CopyRepeats()
        {
            for (var i = 0; i < Repeats.Length; i++)
            {
                if (Repeats[i] >= 0)
                    Results[i] = Results[Repeats[i]];
            }
        }
    }
}
