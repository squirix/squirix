using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.App;

internal sealed class DurableMutationExecutor
{
    private readonly ConcurrentDictionary<string, byte> _inFlight = new(StringComparer.Ordinal);
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
        string? conflictKey,
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
            ? await ExecuteGroupCommitAsync(conflictKey, precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false)
            : await ExecuteMonolithicAsync(precondition, appendJournal, applyMemory, cancellationToken).ConfigureAwait(false);
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

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TResult>(
        string conflictKey,
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

    private async ValueTask<TResult> ExecuteMonolithicAsync<TResult>(
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken) => await _journal.ExecuteUnderSnapshotBarrierAsync(
        async ct =>
        {
            var decision = await precondition(ct).ConfigureAwait(false);
            if (!decision.ShouldApply)
                return decision.Result;

            await appendJournal(ct).ConfigureAwait(false);
            await _journal.AwaitDurabilityCommitAsync(ct).ConfigureAwait(false);
            return await applyMemory(ct).ConfigureAwait(false);
        },
        cancellationToken).ConfigureAwait(false);

    private async ValueTask<DurableMutationPlan<TResult>> PrepareGroupCommitPlanAsync<TResult>(
        string conflictKey,
        GroupCommitExecutionState state,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        CancellationToken cancellationToken)
    {
        if (!_inFlight.TryAdd(conflictKey, 0))
            throw new InvalidOperationException($"Key already exists: {conflictKey}");

        state.Admitted = true;
        try
        {
            var decision = await precondition(cancellationToken).ConfigureAwait(false);
            if (!decision.ShouldApply)
            {
                _ = _inFlight.TryRemove(conflictKey, out _);
                state.Admitted = false;
                return DurableMutationPlan<TResult>.Skip(decision.Result);
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

    private void RollbackGroupCommitBarrierState(string conflictKey, GroupCommitExecutionState state)
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
