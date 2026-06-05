using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Journal append and durability coordination surface for key-value mutations.
/// </summary>
internal interface IJournalCoordinator : IJournalMetrics, IExclusiveMaintenanceExecutor, IAsyncDisposable
{
    event Action? OnAppended;

    int CurrentSegmentIndex { get; }

    bool HasFlushLoopFailure { get; }

    bool IsJournalGroupCommitEnabled { get; }

    ulong NextSequence { get; }

    ValueTask AppendPutAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken);

    ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken);

    ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken);

    void BeginPendingMemoryApply();

    void CompletePendingMemoryApply();

    ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken);

    ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken);

    ValueTask WaitForStartupAsync(CancellationToken cancellationToken);
}
