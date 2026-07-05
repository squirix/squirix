using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.App;

internal sealed class DurableMutationExecutor
{
    private readonly ConcurrentDictionary<CacheKey, byte> _inFlight = new();
    private readonly IJournalCoordinator _journal;

    public DurableMutationExecutor(IJournalCoordinator journal)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public ValueTask<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => ExecuteAsync(null, precondition, appendJournal, applyMemory, cancellationToken);

    public async ValueTask<TResult> ExecuteAsync<TContext, TAppendState, TApplyState, TResult>(
        CacheKey? conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(appendJournal);
        ArgumentNullException.ThrowIfNull(applyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey is not null
            ? await ExecuteGroupCommitAsync(conflictKey.Value, precondition, context, appendState, appendJournal, applyState, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(precondition, context, appendState, appendJournal, applyState, applyMemory, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteAsync<TContext, TState, TResult>(
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
            ? await ExecuteGroupCommitAsync(conflictKey.Value, context, state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(context, state, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsIdempotentDurabilityDeferred() => RpcMutationIdempotencyExecutionScope.Current is not null;

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
                var withState = new GroupCommitApplyWithState<TContext, TApplyState, TResult>
                {
                    Context = context,
                    ApplyState = applyState,
                    ApplyMemory = applyMemory,
                };
                return await _journal.ExecuteUnderSnapshotBarrierAsync(withState, static (s, ct) => s.ApplyMemory(s.Context, s.ApplyState, ct), cancellationToken)
                                     .ConfigureAwait(false);
            }

            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            var withStateDeferred = new GroupCommitApplyWithState<TContext, TApplyState, TResult>
            {
                Context = context,
                ApplyState = applyState,
                ApplyMemory = applyMemory,
            };
            return await _journal.ExecuteUnderSnapshotBarrierAsync(withStateDeferred, static (s, ct) => s.ApplyMemory(s.Context, s.ApplyState, ct), cancellationToken)
                                 .ConfigureAwait(false);
        }
        finally
        {
            if (state.PendingMemoryApply)
                _journal.CompletePendingMemoryApply();
        }
    }

    private async ValueTask<TResult> ApplyGroupCommitPlanAsync<TResult>(
        DurableMutationPlan<TResult> plan,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        if (!plan.ShouldApply)
            return plan.SkipResult!;

        try
        {
            if (IsIdempotentDurabilityDeferred())
            {
                return await _journal.ExecuteUnderSnapshotBarrierAsync(
                    new GroupCommitApplyDirect<TResult> { ApplyMemory = applyMemory },
                    static (s, ct) => s.ApplyMemory(ct),
                    cancellationToken).ConfigureAwait(false);
            }

            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            return await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitApplyDirect<TResult> { ApplyMemory = applyMemory },
                static (s, ct) => s.ApplyMemory(ct),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.PendingMemoryApply)
                _journal.CompletePendingMemoryApply();
        }
    }

    private async ValueTask<TResult> ExecuteAsync<TResult>(
        CacheKey? conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(precondition);
        ArgumentNullException.ThrowIfNull(appendJournal);
        ArgumentNullException.ThrowIfNull(applyMemory);

        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return _journal.IsJournalGroupCommitEnabled && conflictKey is not null
            ? await ExecuteGroupCommitAsync(conflictKey.Value, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TContext, TAppendState, TApplyState, TResult>(
        CacheKey conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareWithAppendState<TContext, TAppendState, TResult>
                {
                    Executor = this,
                    ConflictKey = conflictKey,
                    ExecutionState = state,
                    Precondition = precondition,
                    Context = context,
                    AppendState = appendState,
                    AppendJournal = appendJournal,
                },
                static (s, ct) => s.Executor.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Precondition, s.Context, s.AppendState, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, context, applyState, applyMemory, cancellationToken).ConfigureAwait(false);
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
                new GroupCommitPrepareWithMutationState<TContext, TState, TResult>
                {
                    Executor = this,
                    ConflictKey = conflictKey,
                    ExecutionState = state,
                    Context = context,
                    MutationState = mutationState,
                    Precondition = precondition,
                    AppendJournal = appendJournal,
                },
                static (s, ct) => s.Executor.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Context, s.MutationState, s.Precondition, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, context, mutationState, applyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TResult>(
        CacheKey conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        var state = new GroupCommitExecutionState();
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                new GroupCommitPrepareState<TResult>
                {
                    Executor = this,
                    ConflictKey = conflictKey,
                    ExecutionState = state,
                    Precondition = precondition,
                    AppendJournal = appendJournal,
                },
                static (s, ct) => s.Executor.PrepareGroupCommitPlanCoreAsync(s.ConflictKey, s.ExecutionState, s.Precondition, s.AppendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, applyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private ValueTask<TResult> ExecuteMonolithicAsync<TContext, TAppendState, TApplyState, TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithAppendState<TContext, TAppendState, TApplyState, TResult>
        {
            Executor = this,
            Precondition = precondition,
            Context = context,
            AppendState = appendState,
            AppendJournal = appendJournal,
            ApplyState = applyState,
            ApplyMemory = applyMemory,
        },
        static (s, ct) => s.Executor.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private ValueTask<TResult> ExecuteMonolithicAsync<TContext, TState, TResult>(
        TContext context,
        TState state,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicWithMutationState<TContext, TState, TResult>
        {
            Executor = this,
            Context = context,
            MutationState = state,
            Precondition = precondition,
            AppendJournal = appendJournal,
            ApplyMemory = applyMemory,
        },
        static (s, ct) => s.Executor.ExecuteMonolithicUnderBarrierAsync(s, ct),
        cancellationToken);

    private ValueTask<TResult> ExecuteMonolithicAsync<TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => _journal.ExecuteUnderSnapshotBarrierAsync(
        new MonolithicDirectState<TResult>
        {
            Executor = this,
            Precondition = precondition,
            AppendJournal = appendJournal,
            ApplyMemory = applyMemory,
        },
        static (s, ct) => s.Executor.ExecuteMonolithicUnderBarrierAsync(s, ct),
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

    private async ValueTask<TResult> ExecuteMonolithicUnderBarrierAsync<TResult>(MonolithicDirectState<TResult> state, CancellationToken cancellationToken)
    {
        var decision = await state.Precondition(cancellationToken).ConfigureAwait(false);
        if (!decision.ShouldApply)
            return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

        await state.AppendJournal(cancellationToken).ConfigureAwait(false);
        if (IsIdempotentDurabilityDeferred())
            return await state.ApplyMemory(cancellationToken).ConfigureAwait(false);

        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return await state.ApplyMemory(cancellationToken).ConfigureAwait(false);
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

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanCoreAsync<TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
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
            await appendJournal(cancellationToken).ConfigureAwait(false);
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

    private record struct GroupCommitApplyDirect<TResult>
    {
        public required Func<CancellationToken, ValueTask<TResult>> ApplyMemory;
    }

    private record struct GroupCommitApplyWithState<TContext, TApplyState, TResult>
    {
        public required Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> ApplyMemory;
        public required TApplyState ApplyState;
        public required TContext Context;
    }

    private record struct GroupCommitPrepareState<TResult>
    {
        public required Func<CancellationToken, ValueTask> AppendJournal;
        public required CacheKey ConflictKey;
        public required GroupCommitExecutionState ExecutionState;
        public required DurableMutationExecutor Executor;
        public required Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private record struct GroupCommitPrepareWithAppendState<TContext, TAppendState, TResult>
    {
        public required Func<TContext, TAppendState, CancellationToken, ValueTask> AppendJournal;
        public required TAppendState AppendState;
        public required CacheKey ConflictKey;
        public required TContext Context;
        public required GroupCommitExecutionState ExecutionState;
        public required DurableMutationExecutor Executor;
        public required Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private record struct GroupCommitPrepareWithMutationState<TContext, TState, TResult>
    {
        public required Func<TContext, TState, CancellationToken, ValueTask> AppendJournal;
        public required CacheKey ConflictKey;
        public required TContext Context;
        public required GroupCommitExecutionState ExecutionState;
        public required DurableMutationExecutor Executor;
        public required TState MutationState;
        public required Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private record struct MonolithicDirectState<TResult>
    {
        public required Func<CancellationToken, ValueTask> AppendJournal;
        public required Func<CancellationToken, ValueTask<TResult>> ApplyMemory;
        public required DurableMutationExecutor Executor;
        public required Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private record struct MonolithicWithAppendState<TContext, TAppendState, TApplyState, TResult>
    {
        public required Func<TContext, TAppendState, CancellationToken, ValueTask> AppendJournal;
        public required TAppendState AppendState;
        public required Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> ApplyMemory;
        public required TApplyState ApplyState;
        public required TContext Context;
        public required DurableMutationExecutor Executor;
        public required Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private record struct MonolithicWithMutationState<TContext, TState, TResult>
    {
        public required Func<TContext, TState, CancellationToken, ValueTask> AppendJournal;
        public required Func<TContext, TState, CancellationToken, ValueTask<TResult>> ApplyMemory;
        public required TContext Context;
        public required DurableMutationExecutor Executor;
        public required TState MutationState;
        public required Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> Precondition;
    }

    private sealed class GroupCommitExecutionState
    {
        public bool Admitted { get; set; }

        public bool PendingMemoryApply { get; set; }
    }
}
