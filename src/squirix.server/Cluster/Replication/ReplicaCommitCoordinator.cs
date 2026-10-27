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

    /// <summary>Message prefix of the refusal of an operation identifier reused with a different fingerprint.</summary>
    internal const string FingerprintMismatchCode = "replica_operation_fingerprint_mismatch";

    /// <summary>Message prefix of the refusal of a new operation identity while the group idempotency state is full.</summary>
    internal const string IdempotencyCapacityCode = "replica_idempotency_capacity";

    /// <summary>Message prefix of the refusal of a run that holds an operation identity already known to the group or repeated within the run.</summary>
    internal const string BatchIdentityKnownCode = "replica_batch_identity_known";

    /// <summary>The default <see cref="ShutdownBudget" />.</summary>
    internal static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(5);

    private readonly ReplicaMutationGate _admission;
    private readonly AsyncLock _commitGate = new();

    private readonly List<Task> _ownedTasks = [];
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
    /// <exception cref="InvalidOperationException">
    /// The recovered tail does not cover exactly the entries above the initial commit index, as when the options and the tail were read
    /// from different states of the log; reading both again may succeed.
    /// </exception>
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
        var coversTail = recoveredTail == null
            ? options.InitialLogIndex == options.InitialCommitIndex
            : recoveredTail.FirstIndex == options.InitialCommitIndex + 1 && recoveredTail.LastIndex == options.InitialLogIndex;
        if (!coversTail)
            throw new InvalidOperationException("The recovered tail must hold exactly the durable entries above the initial commit index.");

        // The resolved record answers retries first, so the faulted operation of a re-applied entry is no longer needed.
        PendingApply = new ReplicaPendingApplies(
            pipeline,
            idempotency,
            resolved =>
            {
                lock (OwnedSync)
                    _ = Operations.Remove(new ReplicaOperationKey(resolved.OperationScope, resolved.OperationId));
            },
            recoveredTail);

        Pipeline = pipeline;
        FaultHooks = faultHooks;
        Idempotency = idempotency;
        Quorum = new ReplicaCommitQuorum(options.ReplicaCount, options.InitialCommitIndex, eligibility);

        // The leader durably holds its whole log: its own slot counts through the recovered tail, followers only once verified.
        LeaderReplicaIndex = options.LeaderReplicaIndex;
        Quorum.Admit(LeaderReplicaIndex, options.InitialLogIndex);
        _sequencer = new ReplicaLogIndexSequencer(options.InitialLogIndex);
        _turn = new ReplicaLogTurn(options.InitialLogIndex);
        _admission = new ReplicaMutationGate(options.MaxInFlight);
        _commitIndex = options.InitialCommitIndex;
        ShutdownBudget = DefaultShutdownBudget;
    }

    /// <summary>Initializes the owner callback that reports a fault of owned work that dispose abandoned after the shutdown budget and that failed later.</summary>
    /// <remarks>This namespace does not log; the owner turns the report into an error log. Unset, the fault is only observed.</remarks>
    internal Action<Exception>? AbandonedWorkFaultReporter { private get; init; }

    /// <summary>Initializes the time source of the commit budget.</summary>
    internal required TimeProvider BudgetTimeProvider { private get; init; }

    /// <summary>Gets the task source completed when disposal starts: admission then refuses new commits and background follower observation stops waiting.</summary>
    internal TaskCompletionSource Disposing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets the fault-injection hooks.</summary>
    internal IReplicaCommitFaultHooks FaultHooks { get; }

    /// <summary>Gets the bounded durable group idempotency state.</summary>
    internal GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets the commit operations of the retained and running entries, guarded by <see cref="OwnedSync" />.</summary>
    internal Dictionary<ReplicaOperationKey, ReplicaCommitOperation> Operations { get; } = [];

    /// <summary>Gets the lock that guards <see cref="Operations" /> and the owned tasks.</summary>
    internal Lock OwnedSync { get; } = new();

    /// <summary>Gets the locally appended entries retained until a commit covers them.</summary>
    internal ReplicaPendingApplies PendingApply { get; }

    /// <summary>Gets the durable and memory pipeline.</summary>
    internal IReplicaCommitPipeline Pipeline { get; }

    /// <summary>Gets the quorum that records acknowledgements.</summary>
    internal ReplicaCommitQuorum Quorum { get; }

    /// <summary>Gets the highest log index this coordinator committed, or the commit index it started from.</summary>
    internal ulong CommitIndex => Volatile.Read(ref _commitIndex);

    /// <summary>Gets the zero-based replica slot of the leader; every other slot is a follower.</summary>
    internal int LeaderReplicaIndex { get; }

    /// <summary>Gets a value indicating whether some locally appended entry is not applied to memory yet.</summary>
    /// <remarks>The owner reads it under its commit gate, where no commit body runs, so only background follower observation can change it.</remarks>
    internal bool HasPendingApply => !PendingApply.IsEmpty;

    /// <summary>Gets the highest log index a majority backs: the commit index, or the index the written majority covers and the commit rule lets the leader commit.</summary>
    /// <remarks>
    /// Read outside the commit gate: it takes no gate and only reads thread-safe state, so it may be momentarily behind a commit that is running.
    /// An entry no majority holds yet is not counted, and neither is a recovered entry of an older term that waits for a current-term entry above it.
    /// </remarks>
    internal ulong MajorityIndex
    {
        get
        {
            var commitIndex = CommitIndex;
            return PendingApply.CommittableIndex(commitIndex, Quorum.FindCommitIndex(commitIndex, PendingApply.LastIndex));
        }
    }

    /// <summary>Gets or initializes the time source bounding the first wait of background follower observation.</summary>
    internal required TimeProvider ObserveTimeProvider { get; init; }

    /// <summary>Initializes the longest dispose wait for owned work to make progress before it is abandoned; 5 seconds unless set.</summary>
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

    /// <summary>Initializes the owner callback that reports, with the shutdown budget, a dispose that leaked the gates to a running commit.</summary>
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
        lock (OwnedSync)
        {
            _ = Disposing.TrySetResult();
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    /// <summary>Raises a replica's recorded match index to a position the leader verified against its own log.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <param name="matchIndex">Verified contiguous durable index on the replica.</param>
    /// <remarks>Call before marking the slot ready and while holding the committer gate, so the slot never counts a stale match index.</remarks>
    internal void AdmitReplica(int replicaIndex, ulong matchIndex) => Quorum.Admit(replicaIndex, matchIndex);

    /// <summary>Commits and applies the locally appended entries a recorded majority already covers, outside any caller's commit.</summary>
    /// <returns><see langword="true" /> when no locally appended entry is left unapplied.</returns>
    /// <remarks>
    /// Drives entries whose own commit gave up after the local append: a majority that arrived late, or an apply that failed after
    /// the majority, or an uncommitted tail recovered at start. Every such entry is past its decision point once a majority covers
    /// it, so the work runs on <see cref="CancellationToken.None" /> under the commit gate, ordered with commit bodies; it resolves the
    /// idempotency record of each applied entry. An entry no recorded majority covers stays retained, and so does a recovered entry
    /// of an older term that no current-term entry reaches yet. Prepared outcomes are computed from live memory, so callers that
    /// prepare mutations must not prepare while this returns <see langword="false" />.
    /// A call that runs counts as running work: dispose does not tear the gates and the sequencer down under it, and reports the leak
    /// when the call outlasts the shutdown budget. While dispose drains the owned follower observation, which can apply a late majority,
    /// calls still run; once the drain is over, no new call runs.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Disposal has closed the coordinator to applies and an entry is still to be applied.</exception>
    internal async Task<bool> ApplyCommittedAsync()
    {
        var applied = await ApplyUnlessClosedAsync().ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(applied == null, this);
        return applied.GetValueOrDefault();
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
    /// <exception cref="ReplicaShutdownRefusedException">
    /// The coordinator is draining or disposed; nothing of the mutation was reserved or appended. The disposal check runs before the idempotency lookup: the
    /// committer maps this refusal to no leader only when the log holds no entry of the mutation, and its start path consults a retained entry first.
    /// </exception>
    internal async ValueTask<ReadOnlyMemory<byte>> CommitAsync(PreparedReplicaMutation mutation, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        var key = new ReplicaOperationKey(mutation.OperationScope, mutation.OperationId);
        ReplicaCommitOperation operation;
        Task<Task<ReadOnlyMemory<byte>>>? pendingStarter = null;
        lock (OwnedSync)
        {
            if (Disposing.Task.IsCompleted)
                throw new ReplicaShutdownRefusedException(nameof(ReplicaCommitCoordinator));

            var lookup = Idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out var retained);
            if (lookup == GroupIdempotencyLookup.Found)
                return retained.OutcomePayload;
            if (lookup == GroupIdempotencyLookup.Mismatch)
                throw new InvalidOperationException($"{FingerprintMismatchCode}: operation identifier was reused with a different fingerprint.");
            if (lookup == GroupIdempotencyLookup.Unresolved)
            {
                if (!Operations.TryGetValue(key, out operation!))
                    throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: retained operation requires recovery resolution.");
            }
            else
            {
                (operation, pendingStarter) = this.ReserveOperationLocked(key, mutation, timeout, cancellationToken);
            }

            OwnCore(operation.Resolution);
        }

        // Start the reserved execution after leaving the lock: the synchronous prefix of
        // ExecuteReservedAsync (admission, turn, gates, hooks, pipeline) must never begin
        // under OwnedSync. RunSynchronously executes inline without the thread pool.
        pendingStarter?.RunSynchronously(TaskScheduler.Default);

        try
        {
            var resolved = await operation.Resolution.WaitAsync(cancellationToken).ConfigureAwait(false);
            return operation.Outcome ?? resolved;
        }
        catch (Exception error) when (operation.Attempt.MayBeDurable)
        {
            throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: operation '{mutation.OperationId}'.", error);
        }
    }

    /// <summary>Returns the highest contiguous durable index recorded for one replica.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The replica match index.</returns>
    /// <remarks>
    /// Observes <see cref="ReplicaCommitQuorum.TryRecord" /> progress. The owner's log compaction reads it under the commit gate to keep
    /// every entry a ready follower has not durably acknowledged yet.
    /// </remarks>
    internal ulong MatchIndexFor(int replicaIndex) => Quorum.MatchIndexFor(replicaIndex);

    /// <summary>Drives the committed entries like <see cref="ApplyCommittedAsync" />, reporting a coordinator closed to applies as a result.</summary>
    /// <returns><see langword="null" /> when disposal closed the coordinator to applies and an entry is still to be applied; otherwise whether none is left unapplied.</returns>
    internal async Task<bool?> ApplyUnlessClosedAsync()
    {
        if (PendingApply.IsEmpty)
            return true;

        if (!PendingApply.TryBeginApply())
            return null;

        try
        {
            using var commitGuard = await _commitGate.LockAsync(CancellationToken.None).ConfigureAwait(false);
            var commitIndex = PendingApply.CommittableIndex(_commitIndex, Quorum.FindCommitIndex(_commitIndex, PendingApply.LastIndex));
            if (commitIndex > _commitIndex)
            {
                await Pipeline.AdvanceCommitIndexAsync(commitIndex, CancellationToken.None).ConfigureAwait(false);
                Volatile.Write(ref _commitIndex, commitIndex);
            }

            await PendingApply.ApplyThroughAsync(commitIndex).ConfigureAwait(false);
            return PendingApply.IsEmpty;
        }
        finally
        {
            PendingApply.EndApply();
        }
    }

    /// <summary>Appends and commits a run of entries; only the reserved execution paths call it, after reservation and ownership.</summary>
    /// <param name="run">The dense run of prepared mutations.</param>
    /// <param name="batch">Whether the run is committed as one batch.</param>
    /// <param name="timeout">The commit budget.</param>
    /// <param name="attempt">The attempt state of the run.</param>
    /// <param name="callerCancellation">The caller cancellation token.</param>
    /// <returns>The result payload of the run.</returns>
    internal async Task<ReadOnlyMemory<byte>> ExecuteAsync(PreparedReplicaMutation[] run, bool batch, TimeSpan timeout, ReplicaCommitAttempt attempt, CancellationToken callerCancellation)
    {
        // Counted from the start to the end of the body, so dispose sees a run whose per-entry resolution already removed its operations.
        PendingApply.EnterExecution();
        try
        {
            using var budgetCancellation = new CancellationTokenSource(timeout, BudgetTimeProvider);
            using var preAppendCancellation = CancellationTokenSource.CreateLinkedTokenSource(callerCancellation, budgetCancellation.Token);

            // One lease per run, on the stripe of its first operation.
            using var lease = await _admission.EnterAsync(run[0].OperationId.GetHashCode(StringComparison.Ordinal), preAppendCancellation.Token).ConfigureAwait(false);
            await _turn.WaitAsync(run[0].LogIndex, preAppendCancellation.Token).ConfigureAwait(false);
            using var commitGuard = await _commitGate.LockAsync(preAppendCancellation.Token).ConfigureAwait(false);
            try
            {
                return await ExecuteOrderedAsync(run, batch, attempt, preAppendCancellation.Token, budgetCancellation.Token).ConfigureAwait(false);
            }
            catch (GroupLogAppendUnconfirmedException)
            {
                // The frames of the entry may be on disk although the append failed: a restart recovers them as a tail a majority can commit.
                attempt.MarkAppendUnconfirmed();
                throw;
            }
        }
        finally
        {
            PendingApply.ExitExecution();
        }
    }

    internal void Own(Task task)
    {
        lock (OwnedSync)
            OwnCore(task);
    }

    /// <summary>Tracks an owned task and drops the finished ones.</summary>
    /// <param name="task">The task to own.</param>
    /// <remarks>Call under <see cref="OwnedSync" />.</remarks>
    internal void OwnCore(Task task)
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

    private async Task DisposeCoreAsync()
    {
        try
        {
            // Bounded drain so disposal completes even when a follower task never finishes
            // (e.g., a pipeline ignoring cancellation). Remaining tasks keep exception observation.
            while (true)
            {
                List<Task> tasks;
                lock (OwnedSync)
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
                        // Always the system clock, not ObserveTimeProvider: a test clock nobody advances would park disposal forever
                        // behind a follower that never finishes, and the budget exists to bound exactly that wait.
                        completed = await ReplicaFollowerObservation.TakeNextCompletedAsync(tasks, ShutdownBudget, TimeProvider.System, AbandonedWorkFaultReporter)
                                                                    .ConfigureAwait(false);
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
            // A commit or an apply of committed entries still running here made no progress within the budget and, past its majority,
            // ignores cancellation. It holds or waits on the admission gate, the commit gate, and the sequencer, so they stay with it and
            // are leaked loudly instead of being disposed under it (a disposed semaphore strands its waiters). No new commit or apply can
            // start once admission is closed.
            var running = PendingApply.CloseForApplies();
            lock (OwnedSync)
            {
                foreach (var operation in Operations.Values)
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

    private async Task<ReadOnlyMemory<byte>> ExecuteOrderedAsync(
        PreparedReplicaMutation[] run,
        bool batch,
        ReplicaCommitAttempt attempt,
        CancellationToken preAppendCancellation,
        CancellationToken majorityCancellation)
    {
        var first = run[0];
        var mutation = run[^1];
        using var reservation = await _sequencer.ReserveAsync(run.Length, preAppendCancellation).ConfigureAwait(false);
        if (first.LogIndex != reservation.Index)
            throw new InvalidOperationException("Prepared mutation does not match the reserved group log index.");

        await FaultHooks.OnStageAsync(ReplicaCommitStage.Prepared, mutation, preAppendCancellation).ConfigureAwait(false);
        await Pipeline.AppendLocalAsync(run, preAppendCancellation).ConfigureAwait(false);
        attempt.MarkLocallyAppended();
        reservation.MarkAppended();

        // Every locally appended mutation is retained until a commit covers it, so a later commit whose
        // index jumps over an ambiguous predecessor can still apply the whole range in order. Entries are
        // added and applied only under _commitGate (ordered bodies and ApplyCommittedAsync), which also keeps apply order exact.
        for (var i = 0; i < run.Length; i++)
            PendingApply.Retain(run[i]);

        _turn.Advance(first.LogIndex, reservation.Count);

        // Between the local append and the majority a timeout is not a definite failure (a later commit can still cover the
        // entry), so the budget expiring here yields an unknown outcome. Checks that must be able to refuse the commit belong
        // before the decision point below, never after it.
        await FaultHooks.OnStageAsync(ReplicaCommitStage.LocalAppendDurable, mutation, majorityCancellation).ConfigureAwait(false);
        this.RecordLeaderAcknowledgements(run);

        try
        {
            await this.CollectMajorityAsync(run, majorityCancellation).ConfigureAwait(false);
        }
        catch (Exception error) when (run.Length > 1)
        {
            // When a late acknowledgement already completed the majority of the whole run, the wait did not fail for the run: it
            // passes through to the decision point below. Otherwise a majority that covers only a prefix of the run still commits
            // that prefix, and the outcome of the run stays unknown.
            if (Quorum.FindCommitIndex(_commitIndex, mutation.LogIndex) < mutation.LogIndex)
            {
                await CommitCoveredPrefixAsync(run, error).ConfigureAwait(false);
                throw;
            }
        }

        // Decision point: a durable majority holds the entry, so it is committed. Neither the caller nor the budget may stop
        // the rest; it stops only when the pipeline fails (journal failure latch) or shuts down. A failure here still reports
        // an unknown outcome and keeps the entry in PendingApply, so ApplyCommittedAsync or the next commit applies it in order.
        var committed = CancellationToken.None;
        await FaultHooks.OnStageAsync(ReplicaCommitStage.MajorityReached, mutation, committed).ConfigureAwait(false);
        var previousCommitIndex = _commitIndex;
        var commitIndex = Quorum.FindCommitIndex(previousCommitIndex, mutation.LogIndex);
        await Pipeline.AdvanceCommitIndexAsync(commitIndex, committed).ConfigureAwait(false);
        Volatile.Write(ref _commitIndex, commitIndex);
        await FaultHooks.OnStageAsync(ReplicaCommitStage.CommitIndexDurable, mutation, committed).ConfigureAwait(false);

        // Retained predecessors at or below the new commit index are applied first, in order; they run without this
        // caller's ambient operation scope, since their outcomes belong to other operations. The entries of a run resolve one by one
        // as they are applied; a single entry resolves once its commit returns.
        await PendingApply.ApplyThroughAsync(commitIndex, first.LogIndex, mutation.LogIndex, batch).ConfigureAwait(false);
        await FaultHooks.OnStageAsync(ReplicaCommitStage.MemoryApplied, mutation, committed).ConfigureAwait(false);
        await FaultHooks.OnStageAsync(ReplicaCommitStage.ResponseReady, mutation, committed).ConfigureAwait(false);
        return mutation.OutcomePayload;
    }

    /// <summary>Commits and applies the prefix of a run that a recorded majority covers after the wait for the whole run ended.</summary>
    /// <param name="run">The run whose majority wait failed.</param>
    /// <param name="waitError">The failure of the majority wait.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="AggregateException">The advance or the apply failed; it holds <paramref name="waitError" /> and that failure, so the cause stays visible.</exception>
    /// <remarks>Call under the commit gate. A failed advance or apply leaves the entries retained, as for any commit that gave up; the outcome stays unknown either way.</remarks>
    private async Task CommitCoveredPrefixAsync(PreparedReplicaMutation[] run, Exception waitError)
    {
        var previousCommitIndex = _commitIndex;
        var covered = PendingApply.CommittableIndex(previousCommitIndex, Quorum.FindCommitIndex(previousCommitIndex, run[^1].LogIndex));
        if (covered <= previousCommitIndex)
            return;

        try
        {
            await Pipeline.AdvanceCommitIndexAsync(covered, CancellationToken.None).ConfigureAwait(false);
            Volatile.Write(ref _commitIndex, covered);
            await PendingApply.ApplyThroughAsync(covered, run[0].LogIndex, run[^1].LogIndex, true).ConfigureAwait(false);
        }
        catch (Exception prefixError)
        {
            throw new AggregateException(waitError, prefixError);
        }
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

        internal void Advance(ulong logIndex, int count = 1)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
            TaskCompletionSource<bool> completed;
            lock (_sync)
            {
                if (logIndex != _nextLogIndex)
                    throw new InvalidOperationException("The durable append completed outside the expected group log order.");

                _nextLogIndex += ulong.CreateChecked(count);
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
