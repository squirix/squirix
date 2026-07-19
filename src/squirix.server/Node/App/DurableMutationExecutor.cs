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
    private readonly ConcurrentDictionary<CacheKey, byte> _inFlight = new();
    private readonly IJournalCoordinator _journal;

    internal DurableMutationExecutor(IJournalCoordinator journal)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    internal async ValueTask<TResult> ExecuteAsync<TContext, TAppendState, TApplyState, TResult>(
        CacheKey? conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TContext, TAppendState, TApplyState, TResult> pipeline,
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

    internal async ValueTask<TResult> ExecuteAsync<TContext, TState, TResult>(
        CacheKey? conflictKey,
        TContext context,
        TState state,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(appendJournal);
        ArgumentNullException.ThrowIfNull(applyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey is not null
            ? await ExecuteGroupCommitAsync(conflictKey, context, state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(context, state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsIdempotentDurabilityDeferred() => RpcMutationIdempotencyExecutionAmbient.IsDeferred;

    private async ValueTask<TResult> ApplyGroupCommitPlanAsync<TContext, TApplyState, TResult>(
        DurableMutationPlan<TResult> plan,
        GroupCommitExecutionState state,
        TContext context,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        if (!plan.ShouldApply)
            return plan.SkipResult!;

        try
        {
            if (IsIdempotentDurabilityDeferred())
            {
                var withState = new GroupCommitApplyWithState<TContext, TApplyState, TResult>(context, applyState, applyMemory);
                return await _journal.ExecuteUnderSnapshotBarrierAsync(withState, static (s, ct) => s.ApplyMemory(s.Context, s.ApplyState, ct), cancellationToken)
                                     .ConfigureAwait(false);
            }

            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            var withStateDeferred = new GroupCommitApplyWithState<TContext, TApplyState, TResult>(context, applyState, applyMemory);
            return await _journal.ExecuteUnderSnapshotBarrierAsync(withStateDeferred, static (s, ct) => s.ApplyMemory(s.Context, s.ApplyState, ct), cancellationToken)
                                 .ConfigureAwait(false);
        }
        finally
        {
            if (state.PendingMemoryApply)
                _journal.CompletePendingMemoryApply();
        }
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TContext, TAppendState, TApplyState, TResult>(
        CacheKey conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TContext, TAppendState, TApplyState, TResult> pipeline,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithAppendState<TContext, TAppendState, TResult>(
                    this,
                    conflictKey,
                    state,
                    precondition,
                    pipeline.Context,
                    pipeline.AppendState,
                    pipeline.AppendJournal),
                static (s, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Precondition, s.Context, s.AppendState, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, pipeline.Context, pipeline.ApplyState, pipeline.ApplyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TContext, TState, TResult>(
        CacheKey conflictKey,
        TContext context,
        TState mutationState,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithMutationState<TContext, TState, TResult>(this, conflictKey, state, context, mutationState, precondition, appendJournal),
                static (s, ct) => s.Mutator.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Context, s.MutationState, s.Precondition, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, context, mutationState, applyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private ValueTask<TResult> ExecuteMonolithicAsync<TContext, TAppendState, TApplyState, TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        DurableMutationPipeline<TContext, TAppendState, TApplyState, TResult> pipeline,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithAppendState<TContext, TAppendState, TApplyState, TResult>(
            this,
            precondition,
            pipeline.Context,
            pipeline.AppendState,
            pipeline.AppendJournal,
            pipeline.ApplyState,
            pipeline.ApplyMemory),
        static (s, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private ValueTask<TResult> ExecuteMonolithicAsync<TContext, TState, TResult>(
        TContext context,
        TState state,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithMutationState<TContext, TState, TResult>(this, context, state, precondition, appendJournal, applyMemory),
        static (s, ct) => s.Mutator.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TContext, TAppendState, TApplyState, TResult>(
        MonolithicWithAppendState<TContext, TAppendState, TApplyState, TResult> state,
        CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

        await state.AppendJournal(state.Context, state.AppendState, cancellationToken).ConfigureAwait(false);
        if (IsIdempotentDurabilityDeferred())
            return await state.ApplyMemory(state.Context, state.ApplyState, cancellationToken).ConfigureAwait(false);

        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return await state.ApplyMemory(state.Context, state.ApplyState, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TContext, TState, TResult>(
        MonolithicWithMutationState<TContext, TState, TResult> state,
        CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(state.Context, state.MutationState, cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

        await state.AppendJournal(state.Context, state.MutationState, cancellationToken).ConfigureAwait(false);
        if (IsIdempotentDurabilityDeferred())
            return await state.ApplyMemory(state.Context, state.MutationState, cancellationToken).ConfigureAwait(false);

        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return await state.ApplyMemory(state.Context, state.MutationState, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TContext, TAppendState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
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
                return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false."));
            }

            _journal.BeginPendingMemoryApply();
            state.PendingMemoryApply = true;
            await appendJournal(context, appendState, cancellationToken).ConfigureAwait(false);
            return DurableMutationPlan<TResult>.Apply();
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException or OperationCanceledException)
        {
            RollbackGroupCommitBarrierState(conflictKey, state);
            throw;
        }
    }

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TContext, TState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        TContext context,
        TState mutationState,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken)
    {
        if (!_inFlight.TryAdd(conflictKey, 0))
            throw new InvalidOperationException($"Key already exists: {conflictKey.Namespace}:{conflictKey.Key}");

        state.Admitted = true;
        try
        {
            var decision = await precondition(context, mutationState, cancellationToken).ConfigureAwait(false);
            if (!decision.ShouldApply)
            {
                _ = _inFlight.TryRemove(conflictKey, out _);
                state.Admitted = false;
                return DurableMutationPlan<TResult>.Skip(decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false."));
            }

            _journal.BeginPendingMemoryApply();
            state.PendingMemoryApply = true;
            await appendJournal(context, mutationState, cancellationToken).ConfigureAwait(false);
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

    private sealed record GroupCommitApplyWithState<TContext, TApplyState, TResult>
    {
        internal GroupCommitApplyWithState(TContext context, TApplyState applyState, Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Context = context;
            ApplyState = applyState;
            ApplyMemory = applyMemory;
        }

        internal Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal TApplyState ApplyState { get; }

        internal TContext Context { get; }
    }

    private sealed class GroupCommitExecutionState
    {
        internal bool Admitted { get; set; }

        internal bool PendingMemoryApply { get; set; }
    }

    private sealed record GroupCommitPrepareWithAppendState<TContext, TAppendState, TResult>
    {
        internal GroupCommitPrepareWithAppendState(
            DurableMutationExecutor mutator,
            CacheKey conflictKey,
            GroupCommitExecutionState executionState,
            Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            TContext context,
            TAppendState appendState,
            Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal)
        {
            Mutator = mutator;
            ConflictKey = conflictKey;
            ExecutionState = executionState;
            Precondition = precondition;
            Context = context;
            AppendState = appendState;
            AppendJournal = appendJournal;
        }

        internal Func<TContext, TAppendState, CancellationToken, ValueTask> AppendJournal { get; }

        internal TAppendState AppendState { get; }

        internal CacheKey ConflictKey { get; }

        internal TContext Context { get; }

        internal GroupCommitExecutionState ExecutionState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }
    }

    private sealed record GroupCommitPrepareWithMutationState<TContext, TState, TResult>
    {
        internal GroupCommitPrepareWithMutationState(
            DurableMutationExecutor mutator,
            CacheKey conflictKey,
            GroupCommitExecutionState executionState,
            TContext context,
            TState mutationState,
            Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            Func<TContext, TState, CancellationToken, ValueTask> appendJournal)
        {
            Mutator = mutator;
            ConflictKey = conflictKey;
            ExecutionState = executionState;
            Context = context;
            MutationState = mutationState;
            Precondition = precondition;
            AppendJournal = appendJournal;
        }

        internal Func<TContext, TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal CacheKey ConflictKey { get; }

        internal TContext Context { get; }

        internal GroupCommitExecutionState ExecutionState { get; }

        internal TState MutationState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }
    }

    private sealed record MonolithicWithAppendState<TContext, TAppendState, TApplyState, TResult>
    {
        internal MonolithicWithAppendState(
            DurableMutationExecutor mutator,
            Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            TContext context,
            TAppendState appendState,
            Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
            TApplyState applyState,
            Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            Precondition = precondition;
            Context = context;
            AppendState = appendState;
            AppendJournal = appendJournal;
            ApplyState = applyState;
            ApplyMemory = applyMemory;
        }

        internal Func<TContext, TAppendState, CancellationToken, ValueTask> AppendJournal { get; }

        internal TAppendState AppendState { get; }

        internal Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal TApplyState ApplyState { get; }

        internal TContext Context { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }
    }

    private sealed record MonolithicWithMutationState<TContext, TState, TResult>
    {
        internal MonolithicWithMutationState(
            DurableMutationExecutor mutator,
            TContext context,
            TState mutationState,
            Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
            Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
            Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory)
        {
            Mutator = mutator;
            Context = context;
            MutationState = mutationState;
            Precondition = precondition;
            AppendJournal = appendJournal;
            ApplyMemory = applyMemory;
        }

        internal Func<TContext, TState, CancellationToken, ValueTask> AppendJournal { get; }

        internal Func<TContext, TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

        internal TContext Context { get; }

        internal TState MutationState { get; }

        internal DurableMutationExecutor Mutator { get; }

        internal Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition { get; }
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
