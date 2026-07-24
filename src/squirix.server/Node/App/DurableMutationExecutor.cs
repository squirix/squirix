using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.App;

internal sealed class DurableMutationExecutor
{
    private const string SkipResultRequiresShouldApplyFalse = "SkipResult is only set when ShouldApply is false.";

    private readonly ConcurrentDictionary<CacheKey, byte> _inFlight = new();
    private readonly IJournalCoordinator _journal;

    internal DurableMutationExecutor(IJournalCoordinator journal)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    internal async ValueTask<TResult> ExecuteAsync<TState, TResult>(
        CacheKey? conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(pipeline.AppendJournal);
        ArgumentNullException.ThrowIfNull(pipeline.ApplyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey is not null
            ? await ExecuteGroupCommitAsync(conflictKey, precondition, pipeline, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(precondition, pipeline, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<TResult> ExecuteAsync<TState, TResult>(
        CacheKey? conflictKey,
        TState state,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(appendJournal);
        ArgumentNullException.ThrowIfNull(applyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey is not null
            ? await ExecuteGroupCommitAsync(conflictKey, state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsIdempotentDurabilityDeferred() => RpcMutationIdempotencyExecutionAmbient.IsDeferred;

    private async ValueTask<TResult> ApplyGroupCommitPlanAsync<TState, TResult>(
        DurableMutationPlan<TResult> plan,
        GroupCommitExecutionState state,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        if (!plan.ShouldApply)
            return plan.SkipResult!;

        try
        {
            if (IsIdempotentDurabilityDeferred())
            {
                var withState = new GroupCommitApplyWithState<TState, TResult>(mutationState, applyMemory);
                return await _journal.ExecuteUnderSnapshotBarrierAsync(withState, static (s, ct) => s.ApplyMemory(s.State, ct), cancellationToken)
                                     .ConfigureAwait(false);
            }

            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            var withStateDeferred = new GroupCommitApplyWithState<TState, TResult>(mutationState, applyMemory);
            return await _journal.ExecuteUnderSnapshotBarrierAsync(withStateDeferred, static (s, ct) => s.ApplyMemory(s.State, ct), cancellationToken)
                                 .ConfigureAwait(false);
        }
        finally
        {
            if (state.PendingMemoryApply)
                _journal.CompletePendingMemoryApply();
        }
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TState, TResult>(
        CacheKey conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithPipelineState<TState, TResult>(
                    this,
                    conflictKey,
                    state,
                    precondition,
                    pipeline.State,
                    pipeline.AppendJournal),
                static (s, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Precondition, s.State, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, pipeline.State, pipeline.ApplyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TState, TResult>(
        CacheKey conflictKey,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithMutationState<TState, TResult>(this, conflictKey, state, mutationState, precondition, appendJournal),
                static (s, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.State, s.Precondition, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, mutationState, applyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private ValueTask<TResult> ExecuteMonolithicAsync<TState, TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TState, TResult> pipeline,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithPipelineState<TState, TResult>(
            this,
            precondition,
            pipeline.State,
            pipeline.AppendJournal,
            pipeline.ApplyMemory),
        static (s, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private ValueTask<TResult> ExecuteMonolithicAsync<TState, TResult>(
        TState state,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithMutationState<TState, TResult>(this, state, precondition, appendJournal, applyMemory),
        static (s, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TState, TResult>(
        MonolithicWithPipelineState<TState, TResult> state,
        CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? throw new InvalidOperationException(SkipResultRequiresShouldApplyFalse);

        await state.AppendJournal(state.State, cancellationToken).ConfigureAwait(false);
        if (IsIdempotentDurabilityDeferred())
            return await state.ApplyMemory(state.State, cancellationToken).ConfigureAwait(false);

        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return await state.ApplyMemory(state.State, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TState, TResult>(
        MonolithicWithMutationState<TState, TResult> state,
        CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(state.State, cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? throw new InvalidOperationException(SkipResultRequiresShouldApplyFalse);

        await state.AppendJournal(state.State, cancellationToken).ConfigureAwait(false);
        if (IsIdempotentDurabilityDeferred())
            return await state.ApplyMemory(state.State, cancellationToken).ConfigureAwait(false);

        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return await state.ApplyMemory(state.State, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken)
    {
        if (!_inFlight.TryAdd(conflictKey, 0))
            throw new InvalidOperationException($"Key already exists: {conflictKey.Namespace}:{conflictKey.Key}");

        state.Admitted = true;
        try
        {
            var decision = await precondition(cancellationToken).ConfigureAwait(false);
            if (!decision.ShouldApply)
            {
                _ = _inFlight.TryRemove(conflictKey, out _);
                state.Admitted = false;
                return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? throw new InvalidOperationException(SkipResultRequiresShouldApplyFalse));
            }

            _journal.BeginPendingMemoryApply();
            state.PendingMemoryApply = true;
            await appendJournal(mutationState, cancellationToken).ConfigureAwait(false);
            return DurableMutationPlan<TResult>.Apply();
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException or OperationCanceledException)
        {
            RollbackGroupCommitBarrierState(conflictKey, state);
            throw;
        }
    }

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        TState mutationState,
        Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken)
    {
        if (!_inFlight.TryAdd(conflictKey, 0))
            throw new InvalidOperationException($"Key already exists: {conflictKey.Namespace}:{conflictKey.Key}");

        state.Admitted = true;
        try
        {
            var decision = await precondition(mutationState, cancellationToken).ConfigureAwait(false);
            if (!decision.ShouldApply)
            {
                _ = _inFlight.TryRemove(conflictKey, out _);
                state.Admitted = false;
                return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? throw new InvalidOperationException(SkipResultRequiresShouldApplyFalse));
            }

            _journal.BeginPendingMemoryApply();
            state.PendingMemoryApply = true;
            await appendJournal(mutationState, cancellationToken).ConfigureAwait(false);
            return DurableMutationPlan<TResult>.Apply();
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
            _journal.CompletePendingMemoryApply();

        if (!state.Admitted)
            return;

        _ = _inFlight.TryRemove(conflictKey, out _);
        state.Admitted = false;
    }

    private sealed record GroupCommitApplyWithState<TState, TResult>
    {
        internal GroupCommitApplyWithState(TState state, Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            State = state;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal TState State { get; }
    }

    private sealed class GroupCommitExecutionState
    {
        internal bool Admitted { get; set; }

        internal bool PendingMemoryApply { get; set; }
    }

    private sealed record GroupCommitPrepareWithPipelineState<TState, TResult>
    {
        internal GroupCommitPrepareWithPipelineState(
            DurableMutationExecutor mutator,
            CacheKey conflictKey,
            GroupCommitExecutionState executionState,
            Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
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

        internal Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed record GroupCommitPrepareWithMutationState<TState, TResult>
    {
        internal GroupCommitPrepareWithMutationState(
            DurableMutationExecutor mutator,
            CacheKey conflictKey,
            GroupCommitExecutionState executionState,
            TState state,
            Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            Func<TState, CancellationToken, ValueTask> appendJournal)
        {
            Mutator = mutator;
            ConflictKey = conflictKey;
            ExecutionState = executionState;
            State = state;
            Precondition = precondition;
            AppendJournal = appendJournal;
        }

        internal Func<TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal CacheKey ConflictKey { get; }

        internal GroupCommitExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed record MonolithicWithPipelineState<TState, TResult>
    {
        internal MonolithicWithPipelineState(
            DurableMutationExecutor mutator,
            Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
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

        internal Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    private sealed record MonolithicWithMutationState<TState, TResult>
    {
        internal MonolithicWithMutationState(
            DurableMutationExecutor mutator,
            TState state,
            Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            Func<TState, CancellationToken, ValueTask> appendJournal,
            Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            State = state;
            Precondition = precondition;
            AppendJournal = appendJournal;
            ApplyMemory = applyMemory;
        }

        internal Func<TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }

        internal TState State { get; }
    }

    /// <summary>Result of the journal append phase of a durable mutation.</summary>
    /// <typeparam name="TResult">Mutation result type.</typeparam>
    private sealed record DurableMutationPlan<TResult>
    {
        private DurableMutationPlan(bool shouldApply, TResult? skipResult)
        {
            ShouldApply = shouldApply;
            SkipResult = skipResult;
        }

        /// <summary>Gets a value indicating whether the mutation should continue to durability commit and memory apply.</summary>
        internal bool ShouldApply { get; }

        /// <summary>
        /// Gets the result returned when <see cref="ShouldApply" /> is false.
        /// </summary>
        internal TResult? SkipResult { get; }

        /// <summary>Creates a plan that continues to durability commit and memory apply.</summary>
        /// <returns>An apply plan.</returns>
        internal static DurableMutationPlan<TResult> Apply() => new(true, default);

        /// <summary>Creates a plan that skips durability commit and memory apply.</summary>
        /// <param name="result">Result to return to the caller.</param>
        /// <returns>A skip plan.</returns>
        internal static DurableMutationPlan<TResult> Skip(TResult result) => new(false, result);
    }
}
