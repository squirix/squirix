using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Snapshot barrier and two-phase snapshot cut coordination.</summary>
internal interface IJournalSnapshotBarrier
{
    /// <summary>
    /// Runs a two-phase snapshot cut. Under the mutation gate: flush the journal, record the flush sequence, and invoke
    /// <paramref name="captureUnderBarrier" /> to capture a consistent in-memory view.
    /// The mutation gate is released before <paramref name="buildOutsideBarrier" /> so snapshot serialization and I/O
    /// do not stall durable memory applies.
    /// </summary>
    /// <typeparam name="TState">Caller-owned state passed to both phases.</typeparam>
    /// <typeparam name="TBarrier">Captured view produced under the mutation gate.</typeparam>
    /// <typeparam name="TResult">Final snapshot cut result.</typeparam>
    /// <param name="state">Caller-owned state.</param>
    /// <param name="captureUnderBarrier">Captures a consistent view while the mutation gate is held.</param>
    /// <param name="buildOutsideBarrier">Serializes and publishes the snapshot after the mutation gate is released.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The build phase result.</returns>
    ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        [RequireStaticDelegate] Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        [RequireStaticDelegate] Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken);

    ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>([RequireStaticDelegate] Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken);

    ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        [RequireStaticDelegate] Func<TState, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken);
}
