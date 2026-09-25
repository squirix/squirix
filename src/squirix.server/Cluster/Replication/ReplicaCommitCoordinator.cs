using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owns ordered durable majority commit for one fixed RF greater than one.</summary>
[ThreadSafe]
internal sealed class ReplicaCommitCoordinator : IAsyncDisposable
{
    internal const string CommitOutcomeUnknownCode = "COMMIT_OUTCOME_UNKNOWN";

    private static readonly TimeSpan ObserveTimeout = TimeSpan.FromSeconds(5);

    private readonly ReplicaMutationGate _admission;
    private readonly AsyncLock _commitGate = new();

    /// <summary>Completed when disposal starts: admission then refuses new commits and background follower observation stops waiting.</summary>
    private readonly TaskCompletionSource _disposing = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly IReplicaCommitFaultHooks _faultHooks;
    private readonly GroupIdempotencyState _idempotency;
    private readonly Dictionary<OperationKey, CommitOperation> _operations = [];
    private readonly Lock _ownedSync = new();
    private readonly List<Task> _ownedTasks = [];
    private readonly ReplicaPendingApplies _pendingApply;
    private readonly IReplicaCommitPipeline _pipeline;
    private readonly ReplicaCommitQuorum _quorum;
    private readonly ReplicaLogIndexSequencer _sequencer;
    private readonly ReplicaLogTurn _turn;
    private ulong _commitIndex;
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="ReplicaCommitCoordinator" /> class.</summary>
    /// <param name="options">Fixed group configuration.</param>
    /// <param name="pipeline">Durable and memory pipeline.</param>
    /// <param name="faultHooks">Fault-injection hooks.</param>
    /// <param name="idempotency">Bounded durable group idempotency state.</param>
    /// <param name="eligibility">
    /// Shared participation authority, also handed to repair sessions. When provided, replicas excluded by it
    /// contribute neither acknowledgements nor write-quorum copies until a repair session marks them ready.
    /// Activation wiring (RF&gt;1) owns the shared instance; <see langword="null" /> preserves the pre-activation behavior.
    /// </param>
    /// <param name="recoveredTail">
    /// The durable entries above <see cref="ReplicaCommitCoordinatorOptions.InitialCommitIndex" />, or <see langword="null" /> when the log
    /// tail is fully committed. They are retained with their idempotency pins, and the leader's own slot is admitted at their last index;
    /// <see cref="ApplyCommittedAsync" /> commits and applies them once a recorded majority covers them.
    /// </param>
    /// <exception cref="ArgumentException">The recovered tail does not cover exactly the entries above the initial commit index.</exception>
    internal ReplicaCommitCoordinator(
        ReplicaCommitCoordinatorOptions options,
        IReplicaCommitPipeline pipeline,
        IReplicaCommitFaultHooks faultHooks,
        GroupIdempotencyState idempotency,
        ReplicaEligibility? eligibility = null,
        ReplicaRecoveredTail? recoveredTail = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(faultHooks);
        ArgumentNullException.ThrowIfNull(idempotency);
        var coversTail = recoveredTail == null ? options.InitialLogIndex == options.InitialCommitIndex
            : recoveredTail.FirstIndex == options.InitialCommitIndex + 1 && recoveredTail.LastIndex == options.InitialLogIndex;
        if (!coversTail)
            throw new ArgumentException("The recovered tail must hold exactly the durable entries above the initial commit index.", nameof(recoveredTail));

        // The resolved record answers retries first, so the faulted operation of a re-applied entry is no longer needed.
        _pendingApply = new ReplicaPendingApplies(
            pipeline,
            idempotency,
            resolved =>
            {
                lock (_ownedSync)
                    _ = _operations.Remove(new OperationKey(resolved.OperationScope, resolved.OperationId));
            },
            recoveredTail);

        _pipeline = pipeline;
        _faultHooks = faultHooks;
        _idempotency = idempotency;
        _quorum = new ReplicaCommitQuorum(options.ReplicaCount, options.InitialCommitIndex, eligibility);

        // The leader durably holds its whole log: its own slot counts through the recovered tail, followers only once verified.
        _quorum.Admit(0, options.InitialLogIndex);
        _sequencer = new ReplicaLogIndexSequencer(options.InitialLogIndex);
        _turn = new ReplicaLogTurn(options.InitialLogIndex);
        _admission = new ReplicaMutationGate(options.MaxInFlight);
        _commitIndex = options.InitialCommitIndex;
        ShutdownBudget = ObserveTimeout;
    }

    /// <summary>Gets the time source bounding the first wait of background follower observation; the system clock unless set.</summary>
    /// <remarks>Test seam: production coordinators keep the system clock.</remarks>
    internal TimeProvider ObserveTimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the longest dispose wait for owned work to make progress before it is abandoned; 5 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            field = value;
        }
    }

    /// <summary>Gets the owner callback that reports, with the shutdown budget, a dispose that leaked the gates to a running commit.</summary>
    /// <remarks>This namespace does not log; the owner turns the report into an error log. Unset, the leak is not reported.</remarks>
    internal Action<TimeSpan>? ShutdownLeakReporter { private get; init; }

    /// <summary>Observes all owned post-appending work before releasing resources.</summary>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// Failures already delivered via <see cref="CommitAsync" /> are observed, not rethrown. The wait is bounded by
    /// <see cref="ShutdownBudget" />; a commit still running after it keeps its gates and sequencer, which are leaked and reported
    /// through <see cref="ShutdownLeakReporter" /> instead of being disposed under it.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        lock (_ownedSync)
        {
            _ = _disposing.TrySetResult();
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    /// <summary>Commits a prepared mutation or reports an ambiguous post-appended outcome.</summary>
    /// <param name="mutation">Fully prepared immutable mutation.</param>
    /// <param name="timeout">Budget for queueing, the local append, and the majority wait; work after the majority ignores it.</param>
    /// <param name="cancellationToken">Client cancellation token.</param>
    /// <returns>The exact prepared successful outcome payload.</returns>
    /// <remarks>
    /// The caller assigns log indexes: they must be dense and strictly increasing per group, assigned
    /// under external serialization. A duplicate index fails deterministically; a gap stalls later commits
    /// until the pipeline budget expires. The coordinator never allocates indexes itself.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The mutation is out of order or its post-appending outcome is ambiguous.</exception>
    /// <exception cref="ObjectDisposedException">The coordinator is draining or disposed.</exception>
    internal async ValueTask<ReadOnlyMemory<byte>> CommitAsync(PreparedReplicaMutation mutation, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        var key = new OperationKey(mutation.OperationScope, mutation.OperationId);
        CommitOperation operation;
        Task<Task<ReadOnlyMemory<byte>>>? pendingStarter = null;
        lock (_ownedSync)
        {
            ObjectDisposedException.ThrowIf(_disposing.Task.IsCompleted, this);

            var lookup = _idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out var retained);
            if (lookup == GroupIdempotencyLookup.Found)
                return retained.OutcomePayload;
            if (lookup == GroupIdempotencyLookup.Mismatch)
                throw new InvalidOperationException("Operation identifier was reused with a different fingerprint.");
            if (lookup == GroupIdempotencyLookup.Unresolved)
            {
                if (!_operations.TryGetValue(key, out operation!))
                    throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: retained operation requires recovery resolution.");
            }
            else
            {
                (operation, pendingStarter) = ReserveOperationLocked(key, mutation, timeout, cancellationToken);
            }

            OwnCore(operation.Resolution);
        }

        // Start the reserved execution after leaving the lock: the synchronous prefix of
        // ExecuteReservedAsync (admission, turn, gates, hooks, pipeline) must never begin
        // under _ownedSync. RunSynchronously executes inline without the thread pool.
        pendingStarter?.RunSynchronously(TaskScheduler.Default);

        try
        {
            return await operation.Resolution.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception error) when (operation.Attempt.IsLocallyAppended)
        {
            throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: operation '{mutation.OperationId}'.", error);
        }
    }

    /// <summary>Raises a replica's recorded match index to a position the leader verified against its own log.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <param name="matchIndex">Verified contiguous durable index on the replica.</param>
    /// <remarks>Call before marking the slot ready and while holding the committer gate, so the slot never counts a stale match index.</remarks>
    internal void AdmitReplica(int replicaIndex, ulong matchIndex) => _quorum.Admit(replicaIndex, matchIndex);

    /// <summary>Commits and applies the locally appended entries a recorded majority already covers, outside any caller's commit.</summary>
    /// <returns><see langword="true" /> when no locally appended entry is left unapplied.</returns>
    /// <remarks>
    /// Drives entries whose own commit gave up after the local append: a majority that arrived late, or an apply that failed after
    /// the majority, or an uncommitted tail recovered at start. Every such entry is past its decision point once a majority covers
    /// it, so the work runs on <see cref="CancellationToken.None" /> under the commit gate, ordered with commit bodies; it resolves the
    /// idempotency record of each applied entry. An entry no recorded majority covers stays retained, and so does a recovered entry
    /// of an older term that no current-term entry reaches yet. Prepared outcomes are computed from live memory, so callers that
    /// prepare mutations must not prepare while this returns <see langword="false" />.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The coordinator is disposed.</exception>
    internal async Task<bool> ApplyCommittedAsync()
    {
        if (_pendingApply.IsEmpty)
            return true;

        using var commitGuard = await _commitGate.LockAsync(CancellationToken.None).ConfigureAwait(false);
        var commitIndex = _pendingApply.CommittableIndex(_commitIndex, _quorum.FindCommitIndex(_commitIndex, _pendingApply.LastIndex));
        if (commitIndex > _commitIndex)
        {
            await _pipeline.AdvanceCommitIndexAsync(commitIndex, CancellationToken.None).ConfigureAwait(false);
            Volatile.Write(ref _commitIndex, commitIndex);
        }

        await _pendingApply.ApplyThroughAsync(commitIndex, null).ConfigureAwait(false);
        return _pendingApply.IsEmpty;
    }

    /// <summary>Returns the highest contiguous durable index recorded for one replica.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The replica match index.</returns>
    /// <remarks>Test seam observing <see cref="ReplicaCommitQuorum.TryRecord" /> progress; production paths never poll it.</remarks>
    internal ulong MatchIndexFor(int replicaIndex) => _quorum.MatchIndexFor(replicaIndex);

    private static async Task<FollowerCompletion> AwaitFollowerAsync(int replicaIndex, Task<ReplicaDurableAcknowledgement> followerTask)
    {
        // The raw follower task is observed without deadline cancellation: aborting the majority loop
        // is the outer WaitAsync's job, while late durable responses must still reach RecordAcknowledgement
        // through the background observe path instead of being converted into error completions.
        var singleton = new List<Task<ReplicaDurableAcknowledgement>>(1) { followerTask };
        _ = await Task.WhenAny(singleton).ConfigureAwait(false);
        if (followerTask.IsCompletedSuccessfully)
        {
            // ValueTask wraps the foreign follower task into one owned by this method (RemoteCache idiom).
            var acknowledgement = await new ValueTask<ReplicaDurableAcknowledgement>(followerTask).ConfigureAwait(false);
            return new FollowerCompletion(replicaIndex, acknowledgement);
        }

        // Faults and cancellations are intentionally indistinguishable here: both deweight the replica
        // to lagging without failing the majority. Touching Exception marks the fault observed, so a
        // faulted task never surfaces as unobserved.
        _ = followerTask.Exception;
        return new FollowerCompletion(replicaIndex, null);
    }

    /// <summary>Takes the next completed task, removing it from the pending list.</summary>
    /// <param name="pending">Remaining tasks to observe.</param>
    /// <param name="timeout">The longest wait for any task to complete.</param>
    /// <param name="timeProvider">The time source bounding the wait.</param>
    /// <returns>The completed task.</returns>
    /// <exception cref="TimeoutException">
    /// The bound expired before any task completed; every remaining task got a fault-only exception
    /// observer, so abandoned follower work never surfaces unobserved exceptions.
    /// </exception>
    private static async Task<Task> TakeNextCompletedAsync(List<Task> pending, TimeSpan timeout, TimeProvider timeProvider)
    {
        try
        {
            var completed = await Task.WhenAny(pending).WaitAsync(timeout, timeProvider, CancellationToken.None).ConfigureAwait(false);
            _ = pending.Remove(completed);
            return completed;
        }
        catch (TimeoutException)
        {
            ObserveAbandoned(pending);
            throw;
        }
    }

    /// <summary>Takes the next completed follower task, removing it from the pending list.</summary>
    /// <param name="pending">Remaining follower tasks to observe.</param>
    /// <param name="timeProvider">The time source bounding the wait.</param>
    /// <returns>The completed follower task.</returns>
    /// <exception cref="TimeoutException">
    /// The bound expired before any task completed; every remaining task got a fault-only exception
    /// observer, so abandoned follower work never surfaces unobserved exceptions.
    /// </exception>
    private static async Task<Task<FollowerCompletion>> TakeNextCompletedAsync(List<Task<FollowerCompletion>> pending, TimeProvider timeProvider)
    {
        try
        {
            var completed = await Task.WhenAny(pending).WaitAsync(ObserveTimeout, timeProvider, CancellationToken.None).ConfigureAwait(false);
            _ = pending.Remove(completed);
            return completed;
        }
        catch (TimeoutException)
        {
            ObserveAbandoned(pending);
            throw;
        }
    }

    private static void ObserveAbandoned(IReadOnlyList<Task> remaining)
    {
        foreach (var task in remaining)
        {
            _ = task.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private async Task CollectMajorityAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        var followerCount = _quorum.ReplicaCount - 1;
        var pending = new List<Task<FollowerCompletion>>(followerCount);
        var pendingReplicaIndexes = new HashSet<int>();
        var followerTasks = new HashSet<Task<ReplicaDurableAcknowledgement>>(ReferenceEqualityComparer.Instance);
        try
        {
            for (var replicaIndex = 1; replicaIndex < _quorum.ReplicaCount; replicaIndex++)
            {
                var followerTask = _pipeline.AppendFollowerAsync(replicaIndex, mutation, cancellationToken).AsTask();
                if (!followerTasks.Add(followerTask))
                {
                    _pipeline.RecordLaggingReplica(replicaIndex, mutation.LogIndex);
                    continue;
                }

                pending.Add(AwaitFollowerAsync(replicaIndex, followerTask));
                _ = pendingReplicaIndexes.Add(replicaIndex);
            }

            await _faultHooks.OnStageAsync(ReplicaCommitStage.FollowerFanOutStarted, mutation, cancellationToken).ConfigureAwait(false);
            while (_quorum.FindCommitIndex(_commitIndex, mutation.LogIndex) < mutation.LogIndex && pending.Count > 0)
                await RecordNextAcknowledgementAsync(pending, pendingReplicaIndexes, mutation, cancellationToken).ConfigureAwait(false);

            if (_quorum.FindCommitIndex(_commitIndex, mutation.LogIndex) < mutation.LogIndex)
                throw new InvalidOperationException("A durable majority was not reached before the deadline.");

            foreach (var replicaIndex in pendingReplicaIndexes)
                _pipeline.RecordLaggingReplica(replicaIndex, mutation.LogIndex);
        }
        finally
        {
            if (pending.Count > 0)
                Own(ObserveRemainingFollowersAsync(pending, mutation));
        }
    }

    private async Task DisposeCoreAsync()
    {
        try
        {
            // Bounded drain so disposal completes even when a follower task never finishes
            // (e.g., a pipeline ignoring cancellation). Remaining tasks keep exception observation.
            while (true)
            {
                List<Task> tasks;
                lock (_ownedSync)
                {
                    if (_ownedTasks.Count == 0)
                        break;

                    tasks = [.. _ownedTasks];
                    _ownedTasks.Clear();
                }

                while (tasks.Count > 0)
                {
                    Task completed;
                    try
                    {
                        completed = await TakeNextCompletedAsync(tasks, ShutdownBudget, TimeProvider.System).ConfigureAwait(false);
                    }
                    catch (TimeoutException)
                    {
                        break;
                    }

                    _ = completed.Exception;
                }
            }
        }
        finally
        {
            // A commit still running here made no progress within the budget and, past its majority, ignores cancellation. It holds or
            // waits on the admission gate, the commit gate, and the sequencer, so they stay with it and are leaked loudly instead of
            // being disposed under it (a disposed semaphore strands its waiters). No new commit can start once admission is closed.
            var running = false;
            lock (_ownedSync)
            {
                foreach (var operation in _operations.Values)
                    running |= !operation.Resolution.IsCompleted;
            }

            if (running)
            {
                ShutdownLeakReporter?.Invoke(ShutdownBudget);
            }
            else
            {
                _sequencer.Dispose();
                _admission.Dispose();
                _commitGate.Dispose();
            }
        }
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteAsync(PreparedReplicaMutation mutation, TimeSpan timeout, CommitAttempt attempt, CancellationToken callerCancellation)
    {
        using var budgetCancellation = new CancellationTokenSource(timeout);
        using var preAppendCancellation = CancellationTokenSource.CreateLinkedTokenSource(callerCancellation, budgetCancellation.Token);
        using var lease = await _admission.EnterAsync(mutation.OperationId.GetHashCode(StringComparison.Ordinal), preAppendCancellation.Token).ConfigureAwait(false);
        await _turn.WaitAsync(mutation.LogIndex, preAppendCancellation.Token).ConfigureAwait(false);
        using var commitGuard = await _commitGate.LockAsync(preAppendCancellation.Token).ConfigureAwait(false);
        return await ExecuteOrderedAsync(mutation, attempt, preAppendCancellation.Token, budgetCancellation.Token).ConfigureAwait(false);
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteOrderedAsync(
        PreparedReplicaMutation mutation,
        CommitAttempt attempt,
        CancellationToken preAppendCancellation,
        CancellationToken majorityCancellation)
    {
        using var reservation = await _sequencer.ReserveAsync(preAppendCancellation).ConfigureAwait(false);
        if (mutation.LogIndex != reservation.Index)
            throw new InvalidOperationException("Prepared mutation does not match the reserved group log index.");

        await _faultHooks.OnStageAsync(ReplicaCommitStage.Prepared, mutation, preAppendCancellation).ConfigureAwait(false);
        await _pipeline.AppendLocalAsync(mutation, preAppendCancellation).ConfigureAwait(false);
        attempt.MarkLocallyAppended();
        reservation.MarkAppended();

        // Every locally appended mutation is retained until a commit covers it, so a later commit whose
        // index jumps over an ambiguous predecessor can still apply the whole range in order. Entries are
        // added and applied only under _commitGate (ordered bodies and ApplyCommittedAsync), which also keeps apply order exact.
        _pendingApply.Retain(mutation);
        _turn.Advance(mutation.LogIndex);

        // Between the local append and the majority a timeout is not a definite failure (a later commit can still cover the
        // entry), so the budget expiring here yields an unknown outcome. Checks that must be able to refuse the commit belong
        // before the decision point below, never after it.
        await _faultHooks.OnStageAsync(ReplicaCommitStage.LocalAppendDurable, mutation, majorityCancellation).ConfigureAwait(false);
        var leader = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        _ = _quorum.TryRecord(0, in leader, mutation);
        await CollectMajorityAsync(mutation, majorityCancellation).ConfigureAwait(false);

        // Decision point: a durable majority holds the entry, so it is committed. Neither the caller nor the budget may stop
        // the rest; it stops only when the pipeline fails (journal failure latch) or shuts down. A failure here still reports
        // an unknown outcome and keeps the entry in _pendingApply, so ApplyCommittedAsync or the next commit applies it in order.
        var committed = CancellationToken.None;
        await _faultHooks.OnStageAsync(ReplicaCommitStage.MajorityReached, mutation, committed).ConfigureAwait(false);
        var previousCommitIndex = _commitIndex;
        var commitIndex = _quorum.FindCommitIndex(previousCommitIndex, mutation.LogIndex);
        await _pipeline.AdvanceCommitIndexAsync(commitIndex, committed).ConfigureAwait(false);
        Volatile.Write(ref _commitIndex, commitIndex);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.CommitIndexDurable, mutation, committed).ConfigureAwait(false);

        // Retained predecessors at or below the new commit index are applied first, in order; they run without this
        // caller's ambient operation scope, since their outcomes belong to other operations.
        await _pendingApply.ApplyThroughAsync(commitIndex, mutation).ConfigureAwait(false);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.MemoryApplied, mutation, committed).ConfigureAwait(false);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.ResponseReady, mutation, committed).ConfigureAwait(false);
        return mutation.OutcomePayload;
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteReservedAsync(
        OperationKey key,
        PreparedReplicaMutation mutation,
        TimeSpan timeout,
        CommitAttempt attempt,
        CancellationToken cancellationToken)
    {
        try
        {
            var outcome = await ExecuteAsync(mutation, timeout, attempt, cancellationToken).ConfigureAwait(false);
            _ = _idempotency.TryResolve(mutation.OperationScope, mutation.OperationId, outcome.Span, mutation.LogIndex, mutation.Term);
            lock (_ownedSync)
                _ = _operations.Remove(key);
            return outcome;
        }
        catch (Exception) when (!attempt.IsLocallyAppended)
        {
            _ = _idempotency.TryReleaseUnresolved(mutation.OperationScope, mutation.OperationId, mutation.LogIndex, mutation.Term);
            lock (_ownedSync)
                _ = _operations.Remove(key);
            throw;
        }

        // Post-append failures intentionally keep both pins: the unresolved idempotency record routes
        // same-identity retries to this ambiguous outcome instead of re-executing (which could apply
        // twice if the original did commit), and the faulted _operations entry lets those retries
        // observe it. Releasing either pin early is unsafe; reclamation happens only via journal
        // truncation (GroupIdempotencyState.ReleaseFromIndex). Size capacity for ambiguous-commit
        // bursts and watch GroupIdempotencyState.UnresolvedCount.
    }

    private async Task ObserveRemainingFollowersAsync(List<Task<FollowerCompletion>> pending, PreparedReplicaMutation mutation)
    {
        // A follower answering after the bounded wait can still complete the majority of a retained entry, and the committer refuses
        // writes while one stays unapplied, so observation goes on unbounded; it ends once every follower task completes (they never
        // fault) or when disposal starts, which keeps the drain in DisposeCoreAsync bounded.
        var bounded = true;
        var disposing = _disposing.Task;
        while (pending.Count > 0)
        {
            Task<FollowerCompletion> completed;
            if (bounded)
            {
                try
                {
                    completed = await TakeNextCompletedAsync(pending, ObserveTimeProvider).ConfigureAwait(false);
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
            RecordAcknowledgement(in follower, mutation);

            // A late acknowledgement can complete the majority of an entry whose commit already gave up (an unknown outcome):
            // commit and apply it here instead of leaving it for a later commit that may never come.
            if (_pendingApply.Covers(_quorum.FindCommitIndex(Volatile.Read(ref _commitIndex), mutation.LogIndex)))
                _ = await ApplyCommittedAsync().ConfigureAwait(false);
        }
    }

    private void Own(Task task)
    {
        lock (_ownedSync)
            OwnCore(task);
    }

    private void OwnCore(Task task)
    {
        for (var i = _ownedTasks.Count - 1; i >= 0; i--)
        {
            if (!_ownedTasks[i].IsCompleted)
                continue;

            _ = _ownedTasks[i].Exception;
            _ownedTasks.RemoveAt(i);
        }

        _ownedTasks.Add(task);
    }

    private void RecordAcknowledgement(in FollowerCompletion follower, PreparedReplicaMutation mutation)
    {
        if (follower.Acknowledgement is { } acknowledgement && _quorum.TryRecord(follower.ReplicaIndex, in acknowledgement, mutation))
            return;

        _pipeline.RecordLaggingReplica(follower.ReplicaIndex, mutation.LogIndex);
    }

    private async Task RecordNextAcknowledgementAsync(
        List<Task<FollowerCompletion>> pending,
        HashSet<int> pendingReplicaIndexes,
        PreparedReplicaMutation mutation,
        CancellationToken cancellationToken)
    {
        var completed = await Task.WhenAny(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        _ = pending.Remove(completed);
        var follower = await completed.ConfigureAwait(false);
        _ = pendingReplicaIndexes.Remove(follower.ReplicaIndex);
        RecordAcknowledgement(in follower, mutation);
    }

    /// <summary>Reserves idempotency and registers the commit operation.</summary>
    /// <param name="key">Operation identity key.</param>
    /// <param name="mutation">Prepared mutation to execute.</param>
    /// <param name="timeout">Reservation timeout budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The registered operation and its starter task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when idempotency capacity is exhausted or the operation identifier is reused with a different fingerprint.</exception>
    /// <remarks>Must be called under <see cref="_ownedSync" />.</remarks>
    private (CommitOperation Operation, Task<Task<ReadOnlyMemory<byte>>> Starter) ReserveOperationLocked(
        OperationKey key,
        PreparedReplicaMutation mutation,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var recordKind = string.Equals(mutation.OperationScope, ReplicaExpirationOperationId.OperationScope, StringComparison.Ordinal) ? GroupRecordKind.Expiration
            : GroupRecordKind.UserMutation;
        var reserved = _idempotency.Reserve(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, recordKind, mutation.LogIndex, mutation.Term);

        if (reserved == GroupIdempotencyReserveResult.CapacityExceeded)
            throw new InvalidOperationException("Group idempotency capacity is exhausted.");
        if (reserved == GroupIdempotencyReserveResult.FingerprintMismatch)
            throw new InvalidOperationException("Operation identifier was reused with a different fingerprint.");

        var attempt = new CommitAttempt();
        var starter = new Task<Task<ReadOnlyMemory<byte>>>(() => ExecuteReservedAsync(key, mutation, timeout, attempt, cancellationToken));
        var operation = new CommitOperation(attempt, starter.Unwrap());
        _operations[key] = operation;
        return (operation, starter);
    }

    [Immutable]
    private readonly record struct FollowerCompletion(int ReplicaIndex, ReplicaDurableAcknowledgement? Acknowledgement);

    [Immutable]
    private readonly record struct OperationKey(string Scope, string OperationId);

    [Immutable]
    private sealed record CommitOperation(CommitAttempt Attempt, Task<ReadOnlyMemory<byte>> Resolution);

    [ThreadSafe]
    private sealed class CommitAttempt
    {
        private readonly VolatileBool _locallyAppended = new();

        internal bool IsLocallyAppended => _locallyAppended.Read();

        internal void MarkLocallyAppended() => _locallyAppended.Write(true);
    }

    /// <summary>Orders prepared mutations by their preassigned group log index.</summary>
    [ThreadSafe]
    private sealed class ReplicaLogTurn
    {
        private readonly Lock _sync = new();
        private ulong _nextLogIndex;
        private TaskCompletionSource<bool> _turnAdvanced = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ReplicaLogTurn(ulong lastLogIndex)
        {
            if (lastLogIndex == ulong.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(lastLogIndex), "Replica log index is exhausted.");

            _nextLogIndex = lastLogIndex + 1;
        }

        internal void Advance(ulong logIndex)
        {
            TaskCompletionSource<bool> completed;
            lock (_sync)
            {
                if (logIndex != _nextLogIndex)
                    throw new InvalidOperationException("The durable append completed outside the expected group log order.");

                _nextLogIndex++;
                completed = _turnAdvanced;
                _turnAdvanced = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = completed.TrySetResult(true);
        }

        internal async ValueTask WaitAsync(ulong logIndex, CancellationToken cancellationToken)
        {
            while (true)
            {
                Task turnAdvanced;
                lock (_sync)
                {
                    if (logIndex < _nextLogIndex)
                        throw new InvalidOperationException("Prepared mutation is behind the next group log index.");
                    if (logIndex == _nextLogIndex)
                        return;

                    turnAdvanced = _turnAdvanced.Task;
                }

                await turnAdvanced.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
