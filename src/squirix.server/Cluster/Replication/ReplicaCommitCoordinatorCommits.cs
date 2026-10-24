using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using static Squirix.Server.Cluster.Replication.ReplicaCommitCoordinator;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The commit path of <see cref="ReplicaCommitCoordinator" />: reservation, majority collection and follower observation of single and batched commits.</summary>
internal static class ReplicaCommitCoordinatorCommits
{
    private static readonly TimeSpan ObserveTimeout = TimeSpan.FromSeconds(5);

    extension(ReplicaCommitCoordinator coordinator)
    {
        internal async Task CollectMajorityAsync(PreparedReplicaMutation[] run, CancellationToken cancellationToken)
        {
            var mutation = run[^1];
            var followerCount = coordinator.Quorum.ReplicaCount - 1;
            var pending = new List<Task<FollowerCompletion>>(followerCount);
            var pendingReplicaIndexes = new HashSet<int>();
            try
            {
                ReplicaFollowerFanOut.StartFollowers(coordinator.Quorum, coordinator.LeaderReplicaIndex, coordinator.Pipeline, run, pending, pendingReplicaIndexes, cancellationToken);

                await coordinator.FaultHooks.OnStageAsync(ReplicaCommitStage.FollowerFanOutStarted, mutation, cancellationToken).ConfigureAwait(false);

                await ReplicaFollowerFanOut.AwaitMajorityAsync(coordinator.Quorum, coordinator.Pipeline, pending, pendingReplicaIndexes, run, coordinator.CommitIndex, cancellationToken).ConfigureAwait(false);

                // A follower still pending here is only slower than the majority: its completion is observed in the background, and only a
                // failed or refused append marks it lagging.
                if (coordinator.Quorum.FindCommitIndex(coordinator.CommitIndex, mutation.LogIndex) < mutation.LogIndex)
                    throw new InvalidOperationException("A durable majority was not reached before the deadline.");
            }
            finally
            {
                if (pending.Count > 0)
                    coordinator.Own(coordinator.ObserveRemainingFollowersAsync(pending, run));
            }
        }

        /// <summary>Commits a dense run of prepared mutations under one admission, one local append, and one majority.</summary>
        /// <param name="mutations">Prepared mutations with consecutive log indexes in one term, in log order; at most <see cref="ReplicaCommitQuorum.MaxBufferedAcks" />.</param>
        /// <param name="timeout">Budget for queueing, the local append, and the majority wait; work after the majority ignores it.</param>
        /// <param name="cancellationToken">Client cancellation token.</param>
        /// <returns>An asynchronous operation that completes once every entry is committed and applied; the outcome of each entry is the outcome payload of its mutation.</returns>
        /// <remarks>
        /// The run is copied before this method returns control, so the caller may reuse its list at once. It takes one admission lease and one sequencer reservation and is appended as one log request. Every entry must be a new
        /// operation: a retained or conflicting identity refuses the whole run before anything is reserved. The entries are applied in
        /// log order after one commit-index advance, and each resolves its idempotency record right after it is applied. When the majority
        /// does not cover the whole run before the budget ends, the prefix a majority does cover is still committed and applied, and the
        /// outcome of the whole run is unknown.
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="mutations" /> is empty or longer than <see cref="ReplicaCommitQuorum.MaxBufferedAcks" />.</exception>
        /// <exception cref="InvalidOperationException">
        /// The run is not dense in one term, an identity is already known (<see cref="BatchIdentityKnownCode" />) or reused
        /// (<see cref="FingerprintMismatchCode" />), the idempotency capacity is exhausted (<see cref="IdempotencyCapacityCode" />; nothing was
        /// reserved or appended), or the outcome after the local append is unknown (<see cref="CommitOutcomeUnknownCode" />).
        /// </exception>
        /// <exception cref="ReplicaShutdownRefusedException">The coordinator is draining or disposed; nothing was reserved or appended.</exception>
        internal async ValueTask CommitBatchAsync(IReadOnlyList<PreparedReplicaMutation> mutations, TimeSpan timeout, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(mutations);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

            // The run outlives this call (the execution and the follower observation go on past a caller that gave up), so it is copied
            // once and the copy is passed down; the caller's list is never read again.
            PreparedReplicaMutation[] run = [.. mutations];
            ReplicaCommitCoordinatorCommits.Validate(run);
            Task<Task<ReadOnlyMemory<byte>>> starter;
            ReplicaCommitOperation first;
            lock (coordinator.OwnedSync)
            {
                if (coordinator.Disposing.Task.IsCompleted)
                    throw new ReplicaShutdownRefusedException(nameof(ReplicaCommitCoordinator));

                (first, starter) = coordinator.ReserveRunLocked(run, timeout, cancellationToken);
                coordinator.OwnCore(first.Resolution);
            }

            starter.RunSynchronously(TaskScheduler.Default);
            try
            {
                _ = await first.Resolution.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception error) when (first.Attempt.MayBeDurable)
            {
                throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: batch of {run.Length} operations from '{run[0].OperationId}'.", error);
            }
        }

        internal void RecordLeaderAcknowledgements(PreparedReplicaMutation[] run)
        {
            for (var i = 0; i < run.Length; i++)
            {
                var entry = run[i];
                var leader = new ReplicaDurableAcknowledgement(entry.GroupId, entry.Term, entry.LogIndex, entry.OperationFingerprint, entry.PayloadChecksum, true, true);
                _ = coordinator.Quorum.TryRecord(coordinator.LeaderReplicaIndex, in leader, entry);
            }
        }

        /// <summary>Reserves idempotency and registers the commit operation.</summary>
        /// <param name="key">Operation identity key.</param>
        /// <param name="mutation">Prepared mutation to execute.</param>
        /// <param name="timeout">Reservation timeout budget.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The registered operation and its starter task.</returns>
        /// <exception cref="InvalidOperationException">Thrown when idempotency capacity is exhausted or the operation identifier is reused with a different fingerprint.</exception>
        /// <remarks>Must be called under <see cref="ReplicaCommitCoordinator.OwnedSync" />.</remarks>
        internal (ReplicaCommitOperation Operation, Task<Task<ReadOnlyMemory<byte>>> Starter) ReserveOperationLocked(
            ReplicaOperationKey key,
            PreparedReplicaMutation mutation,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            var recordKind = GroupRecordKinds.FromScope(mutation.OperationScope);
            var reserved = coordinator.Idempotency.Reserve(
                mutation.OperationScope,
                mutation.OperationId,
                mutation.OperationFingerprint.Span,
                recordKind,
                mutation.LogIndex,
                mutation.Term,
                new GroupReserveOptions(false, mutation.OutcomePayload.Length));

            if (reserved == GroupIdempotencyReserveResult.CapacityExceeded)
                throw new InvalidOperationException($"{IdempotencyCapacityCode}: group idempotency capacity is exhausted.");
            if (reserved == GroupIdempotencyReserveResult.FingerprintMismatch)
                throw new InvalidOperationException($"{FingerprintMismatchCode}: operation identifier was reused with a different fingerprint.");

            var attempt = new ReplicaCommitAttempt();
            var starter = new Task<Task<ReadOnlyMemory<byte>>>(() => coordinator.ExecuteReservedAsync(key, mutation, timeout, attempt, cancellationToken));
            var operation = new ReplicaCommitOperation(attempt, starter.Unwrap());
            coordinator.Operations[key] = operation;
            return (operation, starter);
        }

        private async Task<ReadOnlyMemory<byte>> ExecuteReservedAsync(
            ReplicaOperationKey key,
            PreparedReplicaMutation mutation,
            TimeSpan timeout,
            ReplicaCommitAttempt attempt,
            CancellationToken cancellationToken)
        {
            try
            {
                var outcome = await coordinator.ExecuteAsync([mutation], false, timeout, attempt, cancellationToken).ConfigureAwait(false);
                _ = coordinator.Idempotency.TryResolve(mutation.OperationScope, mutation.OperationId, outcome.Span, mutation.LogIndex, mutation.Term);
                lock (coordinator.OwnedSync)
                    _ = coordinator.Operations.Remove(key);
                return outcome;
            }
            catch (Exception) when (!attempt.MayBeDurable)
            {
                _ = coordinator.Idempotency.TryReleaseUnresolved(mutation.OperationScope, mutation.OperationId, mutation.LogIndex, mutation.Term);
                lock (coordinator.OwnedSync)
                    _ = coordinator.Operations.Remove(key);
                throw;
            }

            // Post-append failures intentionally keep both pins: the unresolved idempotency record routes
            // same-identity retries to this ambiguous outcome instead of re-executing (which could apply
            // twice if the original did commit), and the faulted coordinator.Operations entry lets those retries
            // observe it. Releasing either pin early is unsafe; reclamation happens only via journal
            // truncation (GroupIdempotencyState.ReleaseFromIndex). Size capacity for ambiguous-commit bursts.
        }

        private async Task<ReadOnlyMemory<byte>> ExecuteReservedRunAsync(
            ReplicaOperationKey[] keys,
            PreparedReplicaMutation[] mutations,
            TimeSpan timeout,
            ReplicaCommitAttempt attempt,
            CancellationToken cancellationToken)
        {
            try
            {
                // Every entry resolved as it was applied, so nothing is left to resolve here.
                return await coordinator.ExecuteAsync(mutations, true, timeout, attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (!attempt.MayBeDurable)
            {
                lock (coordinator.OwnedSync)
                {
                    ReplicaCommitCoordinatorCommits.ReleasePins(coordinator.Idempotency, mutations, mutations.Length);
                    for (var i = 0; i < keys.Length; i++)
                        _ = coordinator.Operations.Remove(keys[i]);
                }

                throw;
            }

            // Post-append failures keep every pin and operation, exactly as for a single entry.
        }

        private async Task ObserveRemainingFollowersAsync(List<Task<FollowerCompletion>> pending, PreparedReplicaMutation[] run)
        {
            var lastIndex = run[^1].LogIndex;

            // A follower answering after the bounded wait can still complete the majority of a retained entry, and the committer refuses
            // writes while one stays unapplied, so observation goes on unbounded; it ends once every follower task completes (they never
            // fault) or when disposal starts, which keeps the drain in DisposeCoreAsync bounded.
            var bounded = true;
            var disposing = coordinator.Disposing.Task;
            while (pending.Count > 0)
            {
                Task<FollowerCompletion> completed;
                if (bounded)
                {
                    try
                    {
                        completed = await ReplicaFollowerObservation.TakeNextCompletedAsync(pending, ObserveTimeout, coordinator.ObserveTimeProvider, null).ConfigureAwait(false);
                    }
                    catch (TimeoutException)
                    {
                        bounded = false;
                        continue;
                    }
                }
                else
                {
                    var next = Task.WhenAny(pending);
                    if (await Task.WhenAny(next, disposing).ConfigureAwait(false) != next)
                        return;

                    completed = await next.ConfigureAwait(false);
                    _ = pending.Remove(completed);
                }

                var follower = await completed.ConfigureAwait(false);
                ReplicaFollowerFanOut.RecordCompletion(coordinator.Quorum, coordinator.Pipeline, in follower, run);

                // A late acknowledgement can complete the majority of an entry whose commit already gave up (an unknown outcome):
                // commit and apply it here instead of leaving it for a later commit that may never come.
                // The prefix this acknowledgement completes can carry a buffered acknowledgement of a later retained entry, so the search
                // reaches the highest retained index, as the apply does.
                if (!coordinator.PendingApply.Covers(coordinator.Quorum.FindCommitIndex(coordinator.CommitIndex, Math.Max(lastIndex, coordinator.PendingApply.LastIndex))))
                    continue;

                // Closed for applies: the entry stays retained and is recovered from the durable log at the next start. Any other failure
                // faults this task, which dispose observes or, once it abandoned the task, reports to the owner.
                if (await coordinator.ApplyUnlessClosedAsync().ConfigureAwait(false) == null)
                    return;
            }
        }

        /// <summary>Reserves the idempotency of every entry of a run and registers one operation per entry, all sharing one attempt and one execution.</summary>
        /// <param name="mutations">The validated run.</param>
        /// <param name="timeout">Reservation timeout budget.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The operation of the first entry, whose resolution and attempt the run shares, and the starter of the execution.</returns>
        /// <exception cref="InvalidOperationException">
        /// An identity is already known, reused with another fingerprint, or repeated in the run, or the capacity is exhausted. Nothing stays reserved.
        /// </exception>
        /// <remarks>Must be called under <see cref="ReplicaCommitCoordinator.OwnedSync" />.</remarks>
        private (ReplicaCommitOperation Operation, Task<Task<ReadOnlyMemory<byte>>> Starter) ReserveRunLocked(PreparedReplicaMutation[] mutations, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var keys = new ReplicaOperationKey[mutations.Length];
            var seen = new HashSet<ReplicaOperationKey>(keys.Length);
            for (var i = 0; i < keys.Length; i++)
            {
                var mutation = mutations[i];
                keys[i] = new ReplicaOperationKey(mutation.OperationScope, mutation.OperationId);
                var lookup = coordinator.Idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out _);
                if (lookup == GroupIdempotencyLookup.Mismatch)
                    throw new InvalidOperationException($"{FingerprintMismatchCode}: operation identifier was reused with a different fingerprint.");
                if (lookup != GroupIdempotencyLookup.Miss || !seen.Add(keys[i]))
                    throw new InvalidOperationException($"{BatchIdentityKnownCode}: operation '{mutation.OperationId}' of a run is already known; a run holds new operations only.");
            }

            for (var i = 0; i < keys.Length; i++)
            {
                var mutation = mutations[i];
                var reserved = coordinator.Idempotency.Reserve(
                    mutation.OperationScope,
                    mutation.OperationId,
                    mutation.OperationFingerprint.Span,
                    GroupRecordKinds.FromScope(mutation.OperationScope),
                    mutation.LogIndex,
                    mutation.Term,
                    new GroupReserveOptions(false, mutation.OutcomePayload.Length));
                if (reserved == GroupIdempotencyReserveResult.Success)
                    continue;

                ReplicaCommitCoordinatorCommits.ReleasePins(coordinator.Idempotency, mutations, i);
                throw new InvalidOperationException(
                    reserved == GroupIdempotencyReserveResult.CapacityExceeded
                        ? $"{IdempotencyCapacityCode}: group idempotency capacity is exhausted."
                        : $"{FingerprintMismatchCode}: operation identifier was reused with a different fingerprint.");
            }

            var attempt = new ReplicaCommitAttempt();
            var starter = new Task<Task<ReadOnlyMemory<byte>>>(() => coordinator.ExecuteReservedRunAsync(keys, mutations, timeout, attempt, cancellationToken));
            var resolution = starter.Unwrap();
            for (var i = 0; i < keys.Length; i++)
                coordinator.Operations[keys[i]] = new ReplicaCommitOperation(attempt, resolution, mutations[i].OutcomePayload);

            return (coordinator.Operations[keys[0]], starter);
        }
    }

    /// <summary>Checks that a run is non-empty, within the acknowledgement buffer of a slot, and dense in one term.</summary>
    /// <param name="run">The run.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="run" /> is empty or longer than <see cref="ReplicaCommitQuorum.MaxBufferedAcks" />.</exception>
    /// <exception cref="InvalidOperationException">The log indexes of <paramref name="run" /> are not consecutive in one term.</exception>
    private static void Validate(IReadOnlyList<PreparedReplicaMutation> run)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentOutOfRangeException.ThrowIfLessThan(run.Count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(run.Count, ReplicaCommitQuorum.MaxBufferedAcks);

        var first = run[0];
        for (var i = 1; i < run.Count; i++)
        {
            var mutation = run[i];
            if (mutation.Term != first.Term || mutation.LogIndex != first.LogIndex + ulong.CreateChecked(i))
                throw new InvalidOperationException("The mutations of a run must have consecutive log indexes in one term.");
        }
    }

    /// <summary>Releases the unresolved idempotency pins of the first entries of a run.</summary>
    /// <param name="idempotency">The group idempotency state.</param>
    /// <param name="run">The run.</param>
    /// <param name="count">How many entries from the start hold a pin.</param>
    private static void ReleasePins(GroupIdempotencyState idempotency, IReadOnlyList<PreparedReplicaMutation> run, int count)
    {
        for (var i = 0; i < count; i++)
            _ = idempotency.TryReleaseUnresolved(run[i].OperationScope, run[i].OperationId, run[i].LogIndex, run[i].Term);
    }
}
