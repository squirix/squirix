using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App;

[Mutable]
internal sealed class DurableMutationExecutor
{
    private const string KeyAlreadyExistsMessage = "Key already exists.";

    private const string SkipResultRequiresShouldApplyFalse = "SkipResult is only set when ShouldApply is false.";

    private readonly ConcurrentDictionary<CacheKey, byte> _inFlight = new();
    private readonly IJournalCoordinator _journal;

    internal DurableMutationExecutor(IJournalCoordinator journal)
    {
        ArgumentNullException.ThrowIfNull(journal);
        _journal = journal;
    }

    /// <summary>Gets the logger for commit-unknown causes; the host logger unless set.</summary>
    internal ILogger Log { private get; init; } = LogManager.GetLogger<DurableMutationExecutor>();

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
                    static (s, _) =>
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
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithPipelineState<TState, TResult>(this, conflictKey, state, precondition, pipeline.State, pipeline.AppendJournal),
                static (s, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Precondition, s.State, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, pipeline.State, pipeline.ApplyMemory).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private ValueTask<TResult> ExecuteMonolithicAsync<TState, TResult>(
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithPipelineState<TState, TResult>(this, precondition, pipeline.State, pipeline.AppendJournal, pipeline.ApplyMemory),
        static (s, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TState, TResult>(MonolithicWithPipelineState<TState, TResult> state, CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(state.State, cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? ThrowHelper.Throw<TResult>(new InvalidOperationException(SkipResultRequiresShouldApplyFalse));

        try
        {
            await state.AppendJournal(state.State, cancellationToken).ConfigureAwait(false);
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
        LogManager.DurableMutationOutcomeUnknown(Log, cause);
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
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken)
    {
        if (!_inFlight.TryAdd(conflictKey, 0))
            throw new InvalidOperationException(KeyAlreadyExistsMessage);

        state.Admitted = true;
        try
        {
            var decision = await precondition(mutationState, cancellationToken).ConfigureAwait(false);
            if (!decision.ShouldApply)
            {
                _ = _inFlight.TryRemove(conflictKey, out _);
                state.Admitted = false;
                return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? ThrowHelper.Throw<TResult>(new InvalidOperationException(SkipResultRequiresShouldApplyFalse)));
            }

            _journal.InFlightApplyGate.Enter();
            state.PendingMemoryApply = true;
            await appendJournal(mutationState, cancellationToken).ConfigureAwait(false);
            return DurableMutationPlan<TResult>.Apply();
        }
        catch (JournalPostEnqueueFaultException ex)
        {
            // The frame is on the ring, but shutdown or the failure latch closed the journal, so its memory apply never runs in this process:
            // release the apply slot and the key exactly once, as for a failed append, yet report the outcome as unknown, not failed.
            RollbackGroupCommitBarrierState(conflictKey, state);
            throw ReportCommitOutcomeUnknown(ex.InnerException ?? ex);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException or OperationCanceledException)
        {
            RollbackGroupCommitBarrierState(conflictKey, state);
            throw;
        }
    }

    private void RollbackGroupCommitBarrierState(CacheKey conflictKey, GroupCommitExecutionState state)
    {
        if (state.PendingMemoryApply)
            _journal.InFlightApplyGate.Exit();

        if (!state.Admitted)
            return;

        _ = _inFlight.TryRemove(conflictKey, out _);
        state.Admitted = false;
    }

    /// <summary>Result of the journal append phase of a durable mutation.</summary>
    /// <typeparam name="TResult">Mutation result type.</typeparam>
    [Immutable]
    private sealed record DurableMutationPlan<TResult>
    {
        private DurableMutationPlan(bool shouldApply, TResult? skipResult)
        {
            ShouldApply = shouldApply;
            SkipResult = skipResult;
        }

        /// <summary>Gets a value indicating whether the mutation should continue to durability commit and memory apply.</summary>
        internal bool ShouldApply { get; }

        /// <summary>Gets the result returned when <see cref="ShouldApply" /> is false.</summary>
        internal TResult? SkipResult { get; }

        /// <summary>Creates a plan that continues to durability commit and memory apply.</summary>
        /// <returns>An apply plan.</returns>
        internal static DurableMutationPlan<TResult> Apply() => new(true, default);

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
            CacheKey conflictKey,
            GroupCommitExecutionState executionState,
            Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            TState state,
            Func<TState, CancellationToken, ValueTask> appendJournal)
        {
            Mutator = mutator;
            ConflictKey = conflictKey;
            ExecutionState = executionState;
            Precondition = precondition;
            State = state;
            AppendJournal = appendJournal;
        }

        internal Func<TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal CacheKey ConflictKey { get; }

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
            Func<TState, CancellationToken, ValueTask> appendJournal,
            Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            Precondition = precondition;
            State = state;
            AppendJournal = appendJournal;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed class GroupCommitExecutionState
    {
        internal bool Admitted { get; set; }

        /// <summary>Gets or sets a value indicating whether the memory apply began, so its own failures are not reported as commit-unknown.</summary>
        internal bool MemoryApplyStarted { get; set; }

        internal bool PendingMemoryApply { get; set; }
    }
}
