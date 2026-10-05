using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App;

[Mutable]
internal sealed class DurableMutationExecutor
{
    private const string SkipResultRequiresShouldApplyFalse = "SkipResult is only set when ShouldApply is false.";

    private readonly IJournalCoordinator _journal;
    private readonly KeyedAsyncLock<CacheKey> _keyLocks = new();
    private readonly ILogger<DurableMutationExecutor> _logger;

    internal DurableMutationExecutor(IJournalCoordinator journal, ILogger<DurableMutationExecutor> logger)
    {
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(logger);
        _journal = journal;
        _logger = logger;
    }

    /// <summary>Gets the number of keys currently held or awaited by keyed mutations.</summary>
    internal int HeldKeyCount => _keyLocks.Count;

    internal ValueTask<TResult> ExecuteAsync<TState, TResult>(
        CacheKey conflictKey,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(conflictKey);
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(pipeline.AppendJournal);
        ArgumentNullException.ThrowIfNull(pipeline.ApplyMemory);

        // Every mutation holds its key lock and an in-flight apply slot from prepare to apply, with group commit on or off. The pre-apply
        // durability wait never runs under the global mutation gate, so distinct keys share a flush and no other mutation can be applied between
        // this one's journal frame and its memory apply. The path waits for the journal startup itself, so this entry point needs no state machine.
        return ExecuteKeyedAsync(conflictKey, precondition, pipeline, cancellationToken);
    }

    /// <summary>
    /// Determines whether the wait for the cache journal flush before the memory apply is skipped: only for a replicated apply, whose durable
    /// source is the replica group log and not the cache journal. The leader applier marks every such apply, with or without an RPC scope,
    /// so a re-applied committed entry never waits for its own flush; any other write still does.
    /// </summary>
    /// <returns><see langword="true" /> when the cache journal is not the durable source of the running write.</returns>
    private static bool SkipsCacheJournalDurabilityWait() => RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended;

    private async ValueTask<TResult> ExecuteKeyedAsync<TState, TResult>(
        CacheKey conflictKey,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        // Same-key mutations run one after another, as if they ran under one mutation gate: the next precondition sees the
        // previous mutation applied. The key lock is taken before the gate and never under it, and is held until the apply, skip or rollback.
        using var keyLease = await _keyLocks.LockAsync(conflictKey, cancellationToken).ConfigureAwait(false);
        while (true)
        {
            var state = new KeyedExecutionState();
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new KeyedPrepareWithPipelineState<TState, TResult>(this, state, precondition, pipeline.State, pipeline.AppendJournal),
                static (s, ownership, ct) => s.Mutator.PrepareKeyedPlanCoreAsync(s.ExecutionState, s.Precondition, s.State, s.AppendJournal, ownership, ct),
                cancellationToken).ConfigureAwait(false);

            if (plan.IsCutPending)
            {
                // A snapshot cut closed admission: nothing was run or appended, so waiting is cancellable and a cancellation is a definite failure.
                // The gate is released while waiting, which lets the cut take it; the key lock stays held, and the cut takes no key lock.
                await _journal.InFlightApplyGate.WaitOpenAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (!plan.ShouldApply)
                return plan.SkipResult!;

            // The apply and its durability wait run inline rather than in a method of their own: they suspend on every persisted write, and a
            // separate async method would add a state machine allocation to each one.
            try
            {
                // The frame is on the ring: from here only a journal failure or shutdown may stop the apply, never the caller. Memory is applied
                // only once the frame is covered by a completed flush, so a reader never sees a value a crash or a flush failure could lose.
                if (!SkipsCacheJournalDurabilityWait())
                    await _journal.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);

                // The apply slot stays held across the wait above, so a snapshot cut waits for this write and covers it.
                var applyState = new KeyedApplyWithState<TState, TResult>(this, state, pipeline.State, pipeline.ApplyMemory);
                return await _journal.ExecuteUnderSnapshotBarrierAsync(
                        applyState,
                        static (s, _, _) =>
                        {
                            s.ExecutionState.MemoryApplyStarted = true;
                            return s.Mutator.ApplyAfterRingEntryAsync(s.State, s.ApplyMemory);
                        },
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (!state.MemoryApplyStarted)
            {
                // Shutdown or the failure latch ended the durability wait or the gate re-acquire: the outcome is unknown, not failed.
                throw ReportCommitOutcomeUnknown(ex);
            }
            finally
            {
                if (state.PendingMemoryApply)
                    _journal.InFlightApplyGate.Exit();
            }
        }
    }

    /// <summary>
    /// Logs why a mutation whose frame entered the ring ended before its memory apply, and returns the stable commit-unknown contract (gRPC
    /// Unavailable with COMMIT_OUTCOME_UNKNOWN) for the caller.
    /// </summary>
    /// <param name="cause">Shutdown or journal failure that ended the wait.</param>
    /// <returns>The exception to throw instead of <paramref name="cause" />.</returns>
    /// <remarks>The frame may be durable while this process never applied it, and a restart replays it, so the caller must not see a definite failure.</remarks>
    private SquirixException ReportCommitOutcomeUnknown(Exception cause)
    {
        ServerLog.DurableMutationOutcomeUnknown(_logger, cause);
        return ServerOpContract.CommitOutcomeUnknown();
    }

    private async ValueTask<TResult> ApplyAfterRingEntryAsync<TState, TResult>(TState state, Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
    {
        try
        {
            return await applyMemory(state, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Memory can no longer match the journal, and a retry would apply the frame twice: fail-stop instead.
            _journal.FailJournalPipeline(new InvalidOperationException("memory apply failed after its journal frame entered the ring.", ex));
            throw;
        }
    }

    private async ValueTask<DurableMutationPlan<TResult>> PrepareKeyedPlanCoreAsync<TState, TResult>(
        KeyedExecutionState state,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TState mutationState,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> appendJournal,
        AsyncLockOwnership ownership,
        CancellationToken cancellationToken)
    {
        // Admission is decided first, under the gate: a pending snapshot cut refuses new writers so it can drain the admitted ones.
        if (!_journal.InFlightApplyGate.TryEnter())
            return DurableMutationPlan<TResult>.CutPending();

        state.PendingMemoryApply = true;
        DurableMutationCondition<TResult> decision;
        try
        {
            decision = await precondition(mutationState, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            RollbackKeyedBarrierState(state);
            throw;
        }

        if (!decision.ShouldApply)
        {
            RollbackKeyedBarrierState(state);
            return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? ThrowHelper.Throw<TResult>(new InvalidOperationException(SkipResultRequiresShouldApplyFalse)));
        }

        try
        {
            await appendJournal(mutationState, ownership, cancellationToken).ConfigureAwait(false);
            return DurableMutationPlan<TResult>.Apply();
        }
        catch (JournalPostEnqueueFaultException ex)
        {
            // The frame is on the ring, but shutdown or the failure latch closed the journal, so its memory apply never runs in this process:
            // release the apply slot exactly once, as for a failed append, yet report the outcome as unknown, not failed.
            RollbackKeyedBarrierState(state);
            throw ReportCommitOutcomeUnknown(ex.InnerException ?? ex);
        }
        catch
        {
            // Any other failure is definite: a capacity rejection or a refused append never reaches the ring, and a post-enqueue fault is handled
            // above. The apply slot must be released for every exit, or a snapshot cut would wait for it for ever.
            RollbackKeyedBarrierState(state);
            throw;
        }
    }

    private void RollbackKeyedBarrierState(KeyedExecutionState state)
    {
        if (!state.PendingMemoryApply)
            return;

        state.PendingMemoryApply = false;
        _journal.InFlightApplyGate.Exit();
    }

    /// <summary>Result of the journal append phase of a durable mutation.</summary>
    /// <typeparam name="TResult">Mutation result type.</typeparam>
    [Immutable]
    private sealed record DurableMutationPlan<TResult>
    {
        private DurableMutationPlan(bool shouldApply, TResult? skipResult, bool isCutPending = false)
        {
            ShouldApply = shouldApply;
            SkipResult = skipResult;
            IsCutPending = isCutPending;
        }

        /// <summary>Gets a value indicating whether a snapshot cut refused admission, so nothing ran and the caller must wait for the cut and retry.</summary>
        internal bool IsCutPending { get; }

        /// <summary>Gets a value indicating whether the mutation should continue to durability commit and memory apply.</summary>
        internal bool ShouldApply { get; }

        /// <summary>Gets the result returned when <see cref="ShouldApply" /> is false.</summary>
        internal TResult? SkipResult { get; }

        /// <summary>Creates a plan that continues to durability commit and memory apply.</summary>
        /// <returns>An apply plan.</returns>
        internal static DurableMutationPlan<TResult> Apply() => new(true, default);

        /// <summary>Creates a plan for a mutation a pending snapshot cut refused to admit.</summary>
        /// <returns>A cut-pending plan.</returns>
        internal static DurableMutationPlan<TResult> CutPending() => new(false, default, true);

        /// <summary>Creates a plan that skips durability commit and memory apply.</summary>
        /// <param name="result">Result to return to the caller.</param>
        /// <returns>A skip plan.</returns>
        internal static DurableMutationPlan<TResult> Skip(TResult result) => new(false, result);
    }

    [Immutable]
    private sealed record KeyedApplyWithState<TState, TResult>
    {
        internal KeyedApplyWithState(
            DurableMutationExecutor mutator,
            KeyedExecutionState executionState,
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            ExecutionState = executionState;
            State = state;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal KeyedExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal TState State { get; }
    }

    [Immutable]
    private sealed record KeyedPrepareWithPipelineState<TState, TResult>
    {
        internal KeyedPrepareWithPipelineState(
            DurableMutationExecutor mutator,
            KeyedExecutionState executionState,
            Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> appendJournal)
        {
            Mutator = mutator;
            ExecutionState = executionState;
            Precondition = precondition;
            State = state;
            AppendJournal = appendJournal;
        }

        internal Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> AppendJournal { get; }

        internal KeyedExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed class KeyedExecutionState
    {
        /// <summary>Gets or sets a value indicating whether the memory apply began, so its own failures are not reported as commit-unknown.</summary>
        internal bool MemoryApplyStarted { get; set; }

        internal bool PendingMemoryApply { get; set; }
    }
}
