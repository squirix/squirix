using System;
using System.IO;
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

    internal async ValueTask<TResult> ExecuteAsync<TState, TResult>(
        CacheKey? conflictKey,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(pipeline.AppendJournal);
        ArgumentNullException.ThrowIfNull(pipeline.ApplyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey != null
            ? await ExecuteGroupCommitAsync(conflictKey, precondition, pipeline, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(precondition, pipeline, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsIdempotentDurabilityDeferred() => RpcMutationIdempotencyExecutionAmbient.IsDeferred;

    private async ValueTask<TResult> ApplyGroupCommitPlanAsync<TState, TResult>(
        DurableMutationPlan<TResult> plan,
        GroupCommitExecutionState state,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
    {
        if (!plan.ShouldApply)
            return plan.SkipResult!;

        try
        {
            // The frame is on the ring: from here only a journal failure or shutdown may stop the apply, never the caller.
            if (!IsIdempotentDurabilityDeferred())
                await _journal.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);

            // The state is applied to memory right here; only the durability commit above was conditional.
            var applyState = new GroupCommitApplyWithState<TState, TResult>(this, state, mutationState, applyMemory);
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

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TState, TResult>(
        CacheKey conflictKey,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        // Same-key mutations run one after another, as under the single mutation gate of the ungrouped path: the next precondition sees the
        // previous mutation applied. The key lock is taken before the gate and never under it, and is held until the apply, skip or rollback.
        using var keyLease = await _keyLocks.LockAsync(conflictKey, cancellationToken).ConfigureAwait(false);
        while (true)
        {
            var state = new GroupCommitExecutionState();
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithPipelineState<TState, TResult>(this, state, precondition, pipeline.State, pipeline.AppendJournal),
                static (s, ownership, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ExecutionState, s.Precondition, s.State, s.AppendJournal, ownership, ct),
                cancellationToken).ConfigureAwait(false);

            if (!plan.IsCutPending)
                return await ApplyGroupCommitPlanAsync(plan, state, pipeline.State, pipeline.ApplyMemory).ConfigureAwait(false);

            // A snapshot cut closed admission: nothing was run or appended, so waiting is cancellable and a cancellation is a definite failure.
            // The gate is released while waiting, which lets the cut take it; the key lock stays held, and the cut takes no key lock.
            await _journal.InFlightApplyGate.WaitOpenAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private ValueTask<TResult> ExecuteMonolithicAsync<TState, TResult>(
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithPipelineState<TState, TResult>(this, precondition, pipeline.State, pipeline.AppendJournal, pipeline.ApplyMemory),
        static (s, ownership, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ownership, ct),
        cancellationToken);

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TState, TResult>(
        MonolithicWithPipelineState<TState, TResult> state,
        AsyncLockOwnership ownership,
        CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(state.State, cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? ThrowHelper.Throw<TResult>(new InvalidOperationException(SkipResultRequiresShouldApplyFalse));

        try
        {
            await state.AppendJournal(state.State, ownership, cancellationToken).ConfigureAwait(false);
        }
        catch (JournalPostEnqueueFaultException ex)
        {
            // Shutdown or the failure latch faulted the write ack of a frame already on the ring: the outcome is unknown, not failed.
            throw ReportCommitOutcomeUnknown(ex.InnerException ?? ex);
        }

        // The frame is on the ring: from here only a journal failure or shutdown may stop the apply, never the caller.
        if (!IsIdempotentDurabilityDeferred())
            await AwaitDurabilityAfterRingEntryAsync().ConfigureAwait(false);

        return await ApplyAfterRingEntryAsync(state.State, state.ApplyMemory).ConfigureAwait(false);
    }

    private async ValueTask AwaitDurabilityAfterRingEntryAsync()
    {
        try
        {
            await _journal.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Only shutdown or the failure latch ends this wait: the outcome is unknown, not failed.
            throw ReportCommitOutcomeUnknown(ex);
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

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TState, TResult>(
        GroupCommitExecutionState state,
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
            RollbackGroupCommitBarrierState(state);
            throw;
        }

        if (!decision.ShouldApply)
        {
            RollbackGroupCommitBarrierState(state);
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
            RollbackGroupCommitBarrierState(state);
            throw ReportCommitOutcomeUnknown(ex.InnerException ?? ex);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException or OperationCanceledException or JournalCapacityExceededException)
        {
            // A capacity rejection is definite: the journal thread dropped the frame before writing it, so it never becomes durable.
            RollbackGroupCommitBarrierState(state);
            throw;
        }
    }

    private void RollbackGroupCommitBarrierState(GroupCommitExecutionState state)
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
    private sealed record GroupCommitApplyWithState<TState, TResult>
    {
        internal GroupCommitApplyWithState(
            DurableMutationExecutor mutator,
            GroupCommitExecutionState executionState,
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            ExecutionState = executionState;
            State = state;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal GroupCommitExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal TState State { get; }
    }

    [Immutable]
    private sealed record GroupCommitPrepareWithPipelineState<TState, TResult>
    {
        internal GroupCommitPrepareWithPipelineState(
            DurableMutationExecutor mutator,
            GroupCommitExecutionState executionState,
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

        internal GroupCommitExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    [Immutable]
    private sealed record MonolithicWithPipelineState<TState, TResult>
    {
        internal MonolithicWithPipelineState(
            DurableMutationExecutor mutator,
            Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> appendJournal,
            Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            Precondition = precondition;
            State = state;
            AppendJournal = appendJournal;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> AppendJournal { get; }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed class GroupCommitExecutionState
    {
        /// <summary>Gets or sets a value indicating whether the memory apply began, so its own failures are not reported as commit-unknown.</summary>
        internal bool MemoryApplyStarted { get; set; }

        internal bool PendingMemoryApply { get; set; }
    }
}
