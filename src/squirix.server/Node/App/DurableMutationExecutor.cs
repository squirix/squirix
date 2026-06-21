using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
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

    public async ValueTask<TResult> ExecuteAsync<TResult>(
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
            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            return await _journal.ExecuteUnderSnapshotBarrierAsync(
                ct => applyMemory(context, applyState, ct),
                cancellationToken).ConfigureAwait(false);
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
            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            return await _journal.ExecuteUnderSnapshotBarrierAsync(applyMemory, cancellationToken).ConfigureAwait(false);
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
                ct => PrepareGroupCommitPlanAsync(conflictKey, state, precondition, context, appendState, appendJournal, ct),
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
                ct => PrepareGroupCommitPlanAsync(conflictKey, state, context, mutationState, precondition, appendJournal, ct),
                cancellationToken).ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(
                plan,
                state,
                ct => applyMemory(context, mutationState, ct),
                cancellationToken).ConfigureAwait(false);
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
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(ct => PrepareGroupCommitPlanAsync(conflictKey, state, precondition, appendJournal, ct), cancellationToken)
                                     .ConfigureAwait(false);

            return await ApplyGroupCommitPlanAsync(plan, state, applyMemory, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (state.Admitted)
                _ = _inFlight.TryRemove(conflictKey, out _);
        }
    }

    private async ValueTask<TResult> ExecuteMonolithicAsync<TContext, TAppendState, TApplyState, TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => await _journal.ExecuteUnderSnapshotBarrierAsync(
        async ct =>
        {
            var decision = await precondition(ct).ConfigureAwait(false);
            if (!decision.ShouldApply)
                return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

            await appendJournal(context, appendState, ct).ConfigureAwait(false);
            await _journal.AwaitDurabilityCommitAsync(ct).ConfigureAwait(false);
            return await applyMemory(context, applyState, ct).ConfigureAwait(false);
        },
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<TResult> ExecuteMonolithicAsync<TContext, TState, TResult>(
        TContext context,
        TState state,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        Func<TContext, TState, CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => await _journal.ExecuteUnderSnapshotBarrierAsync(
        async ct =>
        {
            var decision = await precondition(context, state, ct).ConfigureAwait(false);
            if (!decision.ShouldApply)
                return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

            await appendJournal(context, state, ct).ConfigureAwait(false);
            await _journal.AwaitDurabilityCommitAsync(ct).ConfigureAwait(false);
            return await applyMemory(context, state, ct).ConfigureAwait(false);
        },
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<TResult> ExecuteMonolithicAsync<TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => await _journal.ExecuteUnderSnapshotBarrierAsync(
        async ct =>
        {
            var decision = await precondition(ct).ConfigureAwait(false);
            if (!decision.ShouldApply)
                return decision.SkipResult ?? throw new InvalidOperationException("SkipResult is only set when ShouldApply is false.");

            await appendJournal(ct).ConfigureAwait(false);
            await _journal.AwaitDurabilityCommitAsync(ct).ConfigureAwait(false);
            return await applyMemory(ct).ConfigureAwait(false);
        },
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanAsync<TContext, TAppendState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken) => await PrepareGroupCommitPlanCoreAsync(
        conflictKey,
        state,
        precondition,
        ct => appendJournal(context, appendState, ct),
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanAsync<TContext, TState, TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        TContext context,
        TState mutationState,
        Func<TContext, TState, CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<TContext, TState, CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken) => await PrepareGroupCommitPlanCoreAsync(
        conflictKey,
        state,
        ct => precondition(context, mutationState, ct),
        ct => appendJournal(context, mutationState, ct),
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanAsync<TResult>(
        CacheKey conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken) => await PrepareGroupCommitPlanCoreAsync(
        conflictKey,
        state,
        precondition,
        appendJournal,
        cancellationToken).ConfigureAwait(false);

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

    private sealed class GroupCommitExecutionState
    {
        public bool Admitted { get; set; }

        public bool PendingMemoryApply { get; set; }
    }
}
