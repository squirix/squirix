using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The combining of the concurrent writes of one group into commit batches, under the commit gate of <see cref="ReplicaGroupCommitter" />.</summary>
/// <remarks>
/// <para>
/// A write that finds no leader leads: it takes the commit gate, takes every write queued by then (up to the limits below) into one batch, prepares them
/// one after another at consecutive log indexes, and commits the batch with one local append and one majority. Then it hands the role to the first write still
/// queued. The writes that queue meanwhile are the next batch, so the batch grows with the load and a lone write commits as before, alone. The batch is formed by
/// <see cref="ReplicaCommitBatchFormer" />.
/// </para>
/// <para>
/// A batch of one write is committed by the single commit of the coordinator. A failure of a longer run after its local append may have left a prefix of it
/// committed, so each write of it is decided on its own from the idempotency record of its operation.
/// </para>
/// </remarks>
internal static class ReplicaGroupCommitterBatches
{
    /// <summary>The most entries one batch holds, so it fits the acknowledgement buffer of a follower slot and the backlog of a sender.</summary>
    internal const int MaxBatchEntries = ReplicaCommitQuorum.MaxBufferedAcks;

    /// <summary>The most canonical payload bytes one batch holds, so it fits one append request of a sender.</summary>
    internal const long MaxBatchBytes = 4L * 1024 * 1024;

    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Queues a write for a commit batch and waits for its outcome.</summary>
        /// <param name="request">The write.</param>
        /// <param name="cancellationToken">Ends the wait of a write that is still queued; a write a leader took is decided whatever the caller does.</param>
        /// <returns>The committed outcome payload of the write.</returns>
        /// <exception cref="OperationCanceledException">The caller gave up while the write was still queued, or while it waited for the commit gate as the leader.</exception>
        internal async Task<ReadOnlyMemory<byte>> SubmitAsync(ReplicaCommitRequest request, CancellationToken cancellationToken)
        {
            var leads = committer.Combiner.Enter(request) || await request.AwaitLeadershipAsync(cancellationToken).ConfigureAwait(false);
            if (leads)
                await committer.LeadAsync(request, cancellationToken).ConfigureAwait(false);

            return request.OutcomeOrThrow();
        }

        /// <summary>Decides a write the disposing committer never admitted: a retained entry of its operation answers it, otherwise it was refused without a leader.</summary>
        /// <param name="request">The write.</param>
        /// <returns>The retained outcome payload of the operation.</returns>
        /// <exception cref="Exception">The failure the write is refused with.</exception>
        internal ReadOnlyMemory<byte> Refused(ReplicaCommitRequest request)
        {
            committer.Refuse(request, ServerOpContract.NoLeaderAuthority());
            return request.OutcomeOrThrow();
        }

        /// <summary>Refuses every write still queued for a batch, as the committer is disposed; a retained entry of a write answers it first.</summary>
        internal void RefuseQueued()
        {
            var queued = committer.Combiner.TakeAll();
            for (var i = 0; i < queued.Count; i++)
                committer.Refuse(queued[i], ServerOpContract.NoLeaderAuthority());
        }

        /// <summary>Decides a write the leader cannot commit: a retained entry of its operation answers it, otherwise it fails with the refusal.</summary>
        /// <param name="request">The write.</param>
        /// <param name="refusal">The failure of a write with no retained entry.</param>
        /// <exception cref="ArgumentOutOfRangeException">The lookup of the write returned an unnamed result; it is recorded as the failure of the write.</exception>
        [SuppressMessage("Design", "CA1031", Justification = "A fault of the lookup is recorded as the failure of the write, whose caller rethrows it.")]
        internal void Refuse(ReplicaCommitRequest request, Exception refusal)
        {
            try
            {
                var log = committer.Registry.TryGetLog(committer.GroupId, out var open) ? open : null;
                switch (request.LookupRetained(log, out var record))
                {
                    case GroupIdempotencyLookup.Found:
                        request.Succeed(record.OutcomePayload);
                        break;
                    case GroupIdempotencyLookup.Mismatch:
                        request.Fail(new ServerOpIdMismatchException());
                        break;
                    case GroupIdempotencyLookup.Unresolved:
                        request.Fail(ServerOpContract.CommitOutcomeUnknown());
                        break;
                    case GroupIdempotencyLookup.Miss:
                        request.Fail(refusal);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(request), "Unsupported lookup result.");
                }
            }
            catch (Exception error)
            {
                request.Fail(error);
            }
        }

        /// <summary>Forms and commits the batch of the leading write, then hands the leadership over.</summary>
        /// <param name="self">The leading write.</param>
        /// <param name="cancellationToken">Ends the wait for the commit gate only.</param>
        /// <returns>An asynchronous operation that completes once the write is decided; a failed commit is recorded on the write, not thrown.</returns>
        /// <exception cref="OperationCanceledException">The caller gave up while the leader waited for the commit gate.</exception>
        private async Task LeadAsync(ReplicaCommitRequest self, CancellationToken cancellationToken)
        {
            AsyncLockHolder guard;
            try
            {
                guard = await committer.Gate.LockAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Nothing of the write was read or written, and no batch took it: another write leads next.
                self.CancelLeading();
                committer.Combiner.HandOver();
                throw;
            }
            catch (ObjectDisposedException)
            {
                // The committer is disposing, or its gate was disposed behind a stuck holder: nothing of the queued writes was read or written. A retained
                // entry of a write still decides its answer first.
                committer.Refuse(self, ServerOpContract.NoLeaderAuthority());
                committer.RefuseQueued();
                return;
            }

            try
            {
                using (guard)
                    await committer.CommitBatchUnderGateAsync(self).ConfigureAwait(false);
            }
            finally
            {
                committer.Combiner.HandOver();
            }
        }

        /// <summary>Starts the coordinator when needed, forms the batch of the leader, and commits it. Every write the leader took is decided before it returns.</summary>
        /// <param name="self">The leading write.</param>
        /// <returns>An asynchronous operation.</returns>
        [SuppressMessage("Design", "CA1031", Justification = "A failure of the batch leader is handed to the callers of the writes it took, who rethrow it; leaving one open would wait for ever.")]
        private async Task CommitBatchUnderGateAsync(ReplicaCommitRequest self)
        {
            self.TakeLeading();
            ReplicaCommitBatchFormer? former = null;
            try
            {
                using var budget = new CancellationTokenSource(committer.CommitBudget, committer.BudgetTimeProvider);
                var starting = committer.EnsureStartedAsync(true, budget.Token);
                if (await starting.CaptureFailureAsync().ConfigureAwait(false) is { } failure)
                {
                    // Whatever refused the start, a retained entry of the write decides the answer: a retry of a committed operation replays its outcome, and
                    // one of an appended but unresolved operation stays unknown. Without a retained entry the refusal stands, except a shutdown refusal (a
                    // disposal, or a log that was closed), which is answered as no leader: the start appends nothing of the write.
                    committer.Refuse(self, committer.ReadRefusal(failure, budget.Token));
                    return;
                }

                var (coordinator, factory) = await starting.ConfigureAwait(false);
                former = new ReplicaCommitBatchFormer(committer, factory);
                await former.FormAsync(self, budget.Token).ConfigureAwait(false);
                if (former.Entries.Count > 0)
                    await committer.CommitEntriesAsync(coordinator, former.Entries).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                // Nothing escapes the steps above on purpose; this keeps a fault of this code from leaving a caller waiting for ever.
                var failed = false;
                if (!self.IsCompleted)
                {
                    self.Fail(error);
                    failed = true;
                }

                for (var i = 0; former != null && i < former.Taken.Count; i++)
                {
                    if (former.Taken[i].IsCompleted)
                        continue;

                    former.Taken[i].Fail(failed ? FreshFailure(error) : error);
                    failed = true;
                }
            }
        }

        /// <summary>Commits the batch: one write through the single commit, several through one run.</summary>
        /// <param name="coordinator">The running coordinator.</param>
        /// <param name="entries">The writes of the batch with their prepared mutations.</param>
        /// <returns>An asynchronous operation; every write of the batch is decided when it completes.</returns>
        [SuppressMessage("Design", "CA1031", Justification = "The failure of the commit is recorded on the write, whose caller rethrows it.")]
        private async Task CommitEntriesAsync(ReplicaCommitCoordinator coordinator, List<(ReplicaCommitRequest Request, PreparedReplicaMutation Mutation)> entries)
        {
            committer.CountCommitRound(entries.Count);
            if (entries.Count == 1)
            {
                var (request, mutation) = entries[0];
                try
                {
                    request.Succeed(await committer.CommitWithPreAppendResyncAsync(coordinator, mutation).ConfigureAwait(false));
                }
                catch (Exception error)
                {
                    request.Fail(error);
                }

                return;
            }

            await committer.CommitRunAsync(coordinator, entries).ConfigureAwait(false);
        }

        /// <summary>Commits a run of independent writes and decides each of them.</summary>
        /// <param name="coordinator">The running coordinator.</param>
        /// <param name="entries">The writes with their prepared mutations: at least two, consecutive from the next log index.</param>
        /// <returns>An asynchronous operation; every write of the run is decided when it completes.</returns>
        /// <remarks>
        /// Success decides every write with the outcome of its mutation. A refusal before the append is the same for every write of the run. A failure after the
        /// local append may have left a prefix of the run committed, so each write is decided on its own from the idempotency record of its operation: the
        /// recorded outcome when its entry is committed, otherwise an unknown outcome.
        /// </remarks>
        [SuppressMessage("Design", "CA1031", Justification = "The failure of the run is recorded on each write of it, whose callers rethrow it.")]
        private async Task CommitRunAsync(ReplicaCommitCoordinator coordinator, List<(ReplicaCommitRequest Request, PreparedReplicaMutation Mutation)> entries)
        {
            var run = new PreparedReplicaMutation[entries.Count];
            for (var i = 0; i < run.Length; i++)
                run[i] = entries[i].Mutation;

            var failedBefore = committer.LocalAppends.Snapshot().Failed;
            try
            {
                await coordinator.CommitBatchAsync(run, committer.CommitBudget, CancellationToken.None).ConfigureAwait(false);
                for (var i = 0; i < entries.Count; i++)
                    entries[i].Request.Succeed(run[i].OutcomePayload);
            }
            catch (Exception error) when (ReplicaGroupCommitterCommits.IsPostAppendOutcome(error))
            {
                if (error.InnerException is GroupLogAppendUnconfirmedException unconfirmed)
                {
                    // The own append failed with its frames possibly on disk: the interrupted append may leave the durable log ahead of the
                    // pipeline positions, so rebuild them on the next start, and report the fault of the log like any refused local append.
                    committer.DropStartedState();
                    committer.ReportLocalAppendFault(unconfirmed, failedBefore);
                }

                ServerLog.ReplicaCommitOutcomeUnknown(committer.Log, error);
                committer.DecideAfterAppendFailure(entries);
            }
            catch (Exception error)
            {
                var mapped = await committer.MapPreAppendFailureAsync(error, run[0], failedBefore, false).ConfigureAwait(false);
                if (mapped is SquirixException { Code: SquirixErrorCode.CommitOutcomeUnknown })
                {
                    committer.DecideAfterAppendFailure(entries);
                    return;
                }

                var failure = mapped ?? error;
                entries[0].Request.Fail(failure);
                for (var i = 1; i < entries.Count; i++)
                    entries[i].Request.Fail(FreshFailure(failure));
            }
        }

        /// <summary>Counts one commit round of a batch or of a single write.</summary>
        /// <param name="entries">The number of writes the round carries.</param>
        private void CountCommitRound(int entries)
        {
            committer.Combiner.CountRound(entries);
            committer.Metrics?.CommitBatches.Record(committer.Probe.SelfId, committer.GroupId, entries);
        }

        /// <summary>Decides each write of a run after a failure past the local append, from the idempotency record of its operation.</summary>
        /// <param name="entries">The writes of the run with their prepared mutations.</param>
        private void DecideAfterAppendFailure(List<(ReplicaCommitRequest Request, PreparedReplicaMutation Mutation)> entries)
        {
            var log = committer.Registry.TryGetLog(committer.GroupId, out var open) ? open : null;
            for (var i = 0; i < entries.Count; i++)
            {
                var (request, mutation) = entries[i];
                if (log != null && log.Idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out var record) == GroupIdempotencyLookup.Found)
                {
                    request.Succeed(record.OutcomePayload);
                    continue;
                }

                // A durable majority may hold the entry: the reservation stays, and a retry of the operation is told the same until a commit resolves it.
                committer.Metrics?.ReportUnknownCommit(GroupRecordKinds.FromScope(mutation.OperationScope));
                request.Fail(ServerOpContract.CommitOutcomeUnknown());
            }
        }

        /// <summary>Maps the failure of a start to the answer of a write that has no retained entry.</summary>
        /// <param name="failure">The failure of the start.</param>
        /// <param name="budget">The commit budget token.</param>
        /// <returns>The failure to answer with.</returns>
        private Exception ReadRefusal(Exception failure, CancellationToken budget)
        {
            if (failure is ObjectDisposedException || (committer.Registry.TryGetLog(committer.GroupId, out var log) && log.IsDisposed))
                return ServerOpContract.NoLeaderAuthority();

            // The start runs on the budget only, so a cancellation here is the budget expiring before anything was appended: a definite refusal.
            return failure is OperationCanceledException && budget.IsCancellationRequested
                ? ServerOpContract.TooManyRequests(ReplicaGroupCommitter.CommitBudgetRefusalReason)
                : failure;
        }
    }

    /// <summary>Gives another write of the batch its own copy of a contract failure, so no exception instance is thrown by several callers.</summary>
    /// <param name="shared">The failure of the batch.</param>
    /// <returns>A new exception of the same contract, or <paramref name="shared" /> itself when it has no cheap copy.</returns>
    private static Exception FreshFailure(Exception shared) => shared switch
    {
        RpcException rpc => new RpcException(rpc.Status, rpc.Trailers),
        SquirixException { Detail: not null } squirix => new SquirixException(squirix.Code, squirix.Error, squirix.Detail),
        ServerOpIdMismatchException => new ServerOpIdMismatchException(),
        _ => shared,
    };
}
