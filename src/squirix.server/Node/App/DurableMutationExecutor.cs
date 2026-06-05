using System;
using System.Collections.Concurrent;
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

    private async ValueTask<TResult> ExecuteGroupCommitAsync<TResult>(
        string conflictKey,
        Func<CancellationToken, ValueTask<DurableMutationCondition<TResult>>> precondition,
        Func<CancellationToken, ValueTask> appendJournal,
        Func<CancellationToken, ValueTask<TResult>> applyMemory,
        CancellationToken cancellationToken)
    {
        var admitted = false;
        var pendingMemoryApply = false;
        try
        {
            var plan = await _journal.ExecuteUnderSnapshotBarrierAsync(
                async ct =>
                {
                    if (!_inFlight.TryAdd(conflictKey, 0))
                        throw new InvalidOperationException($"Key already exists: {conflictKey}");

                    admitted = true;
                    try
                    {
                        var decision = await precondition(ct).ConfigureAwait(false);
                        if (!decision.ShouldApply)
                        {
                            _ = _inFlight.TryRemove(conflictKey, out _);
                            admitted = false;
                            return DurableMutationPlan<TResult>.Skip(decision.Result);
                        }

                        _journal.BeginPendingMemoryApply();
                        pendingMemoryApply = true;
                        await appendJournal(ct).ConfigureAwait(false);
                        return DurableMutationPlan<TResult>.Apply();
                    }
                    catch
                    {
                        if (pendingMemoryApply)
                            _journal.CompletePendingMemoryApply();

                        if (!admitted)
                            throw;

                        _ = _inFlight.TryRemove(conflictKey, out _);
                        admitted = false;
                        throw;
                    }
                },
                cancellationToken).ConfigureAwait(false);

            if (!plan.ShouldApply)
                return plan.SkipResult!;

            try
            {
                await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
                return await _journal.ExecuteUnderSnapshotBarrierAsync(applyMemory, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (pendingMemoryApply)
                    _journal.CompletePendingMemoryApply();
            }
        }
        finally
        {
            if (admitted)
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
}
