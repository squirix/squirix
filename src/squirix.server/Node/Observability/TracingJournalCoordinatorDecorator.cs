using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Observability;

/// <summary>Adds OpenTelemetry spans around journal coordinator operations.</summary>
[Immutable]
internal sealed class TracingJournalCoordinatorDecorator : IJournalCoordinator
{
    private readonly EventHandler _forwardOnAppended;
    private readonly IJournalCoordinator _inner;
    private readonly IJournalOperationTracer _tracer;

    internal TracingJournalCoordinatorDecorator(IJournalCoordinator inner, IJournalOperationTracer tracer)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(tracer);
        _inner = inner;
        _tracer = tracer;
        _forwardOnAppended = ForwardOnAppended;
        _inner.OnAppended += _forwardOnAppended;
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => _inner.AppendedBytes;

    public long AppendedOps => _inner.AppendedOps;

    public bool AtHighWater => _inner.AtHighWater;

    public int CurrentSegmentIndex => _inner.CurrentSegmentIndex;

    public long HighWaterBytes => _inner.HighWaterBytes;

    public QuiescenceGate InFlightApplyGate => _inner.InFlightApplyGate;

    public bool IsJournalGroupCommitEnabled => _inner.IsJournalGroupCommitEnabled;

    public long MaxBytes => _inner.MaxBytes;

    public ulong NextSequence => _inner.NextSequence;

    public double RecentAppendLatencyMs => _inner.RecentAppendLatencyMs;

    public long UsedBytes => _inner.UsedBytes;

    public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, Action? appended, CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.IdempotencyOutcome,
                Enrich(null),
                (operationId, fingerprint, responseBytes, appended, cancellationToken),
                static (inner, args) => inner.AppendIdempotencyOutcomeAsync(args.operationId, args.fingerprint, args.responseBytes, args.appended, args.cancellationToken))
            : _inner.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, appended, cancellationToken);

    public ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.Put,
                JournalCoordinatorTracing.ForKey(_inner, key, entryBytes.Length),
                (ownership, key, entryBytes, cancellationToken),
                static (inner, args) => inner.AppendPutAsync(args.ownership, args.key, args.entryBytes, args.cancellationToken))
            : _inner.AppendPutAsync(ownership, key, entryBytes, cancellationToken);

    public ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.Remove,
                JournalCoordinatorTracing.ForKey(_inner, key),
                (ownership, key, cancellationToken),
                static (inner, args) => inner.AppendRemoveAsync(args.ownership, args.key, args.cancellationToken))
            : _inner.AppendRemoveAsync(ownership, key, cancellationToken);

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) =>
        _tracer.IsEnabled ? AwaitDurabilityCommitTracedAsync(cancellationToken) : _inner.AwaitDurabilityCommitAsync(cancellationToken);

    /// <summary>Detaches from the inner journal without disposing it.</summary>
    /// <returns>A completed task.</returns>
    /// <remarks>
    /// The decorator does not own the journal: its owner disposes it once. The container tracks this decorator under several
    /// registrations and disposes it before that owner, so passing the dispose through would let a journal that fails to shut
    /// down abort the disposal of every other service.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        _inner.OnAppended -= _forwardOnAppended;
        return ValueTask.CompletedTask;
    }

    public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.SnapshotCut,
                Enrich(null),
                (state, captureUnderBarrier, buildOutsideBarrier, cancellationToken),
                static (inner, args) => inner.ExecuteSnapshotCutAsync(args.state, args.captureUnderBarrier, args.buildOutsideBarrier, args.cancellationToken))
            : _inner.ExecuteSnapshotCutAsync(state, captureUnderBarrier, buildOutsideBarrier, cancellationToken);

    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(
        Func<AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.UnderSnapshotBarrier,
                Enrich(null),
                (action, cancellationToken),
                static (inner, args) => inner.ExecuteUnderSnapshotBarrierAsync(args.action, args.cancellationToken))
            : _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken);

    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.UnderSnapshotBarrier,
                Enrich(null),
                (state, action, cancellationToken),
                static (inner, args) => inner.ExecuteUnderSnapshotBarrierAsync(args.state, args.action, args.cancellationToken))
            : _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

    public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(
        TState state,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> action,
        CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(
                JournalOperationKind.UnderSnapshotBarrier,
                Enrich(null),
                (state, action, cancellationToken),
                static (inner, args) => inner.ExecuteUnderSnapshotBarrierAsync(args.state, args.action, args.cancellationToken))
            : _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

    public void FailJournalPipeline(Exception reason) => _inner.FailJournalPipeline(reason);

    public Exception? GetJournalThreadFailure() => _inner.GetJournalThreadFailure();

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) =>
        _tracer.IsEnabled
            ? TracedAsync(JournalOperationKind.WaitForStartup, Enrich(null), cancellationToken, static (inner, token) => inner.WaitForStartupAsync(token))
            : _inner.WaitForStartupAsync(cancellationToken);

    /// <summary>Waits for durability inside a span that starts and ends in this async method.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes when the inner wait does.</returns>
    /// <remarks>
    /// Without a listener the decorator returns the inner wait as is and skips a state machine. With one, the span must not start in the
    /// synchronous caller: its activity would stay the caller's current one after the await. Every traced operation follows this shape.
    /// </remarks>
    private async ValueTask AwaitDurabilityCommitTracedAsync(CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.AwaitDurabilityCommit, in traceContext);
        await _inner.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private JournalOperationTraceContext? Enrich(JournalOperationTraceContext? context) => JournalCoordinatorTracing.WithDurability(_inner, in context);

    private void ForwardOnAppended(object? sender, EventArgs e) => OnAppended?.Invoke(this, e);

    /// <summary>Runs an inner operation inside a span that starts and ends in this async method.</summary>
    /// <typeparam name="TState">The operation argument type.</typeparam>
    /// <param name="kind">The traced operation kind.</param>
    /// <param name="context">The trace context of the span.</param>
    /// <param name="state">The operation arguments.</param>
    /// <param name="invoke">Calls the inner journal.</param>
    /// <returns>A task that completes when the inner operation does.</returns>
    private async ValueTask TracedAsync<TState>(
        JournalOperationKind kind,
        JournalOperationTraceContext? context,
        TState state,
        Func<IJournalCoordinator, TState, ValueTask> invoke)
    {
        using var scope = _tracer.Begin(kind, in context);
        await invoke(_inner, state).ConfigureAwait(false);
    }

    /// <summary>Runs an inner operation inside a span that starts and ends in this async method.</summary>
    /// <typeparam name="TState">The operation argument type.</typeparam>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="kind">The traced operation kind.</param>
    /// <param name="context">The trace context of the span.</param>
    /// <param name="state">The operation arguments.</param>
    /// <param name="invoke">Calls the inner journal.</param>
    /// <returns>A task that yields the inner result.</returns>
    private async ValueTask<TResult> TracedAsync<TState, TResult>(
        JournalOperationKind kind,
        JournalOperationTraceContext? context,
        TState state,
        Func<IJournalCoordinator, TState, ValueTask<TResult>> invoke)
    {
        using var scope = _tracer.Begin(kind, in context);
        return await invoke(_inner, state).ConfigureAwait(false);
    }

    /// <summary>Helpers for tracing journal coordinator operations through <see cref="IJournalOperationTracer" />.</summary>
    private static class JournalCoordinatorTracing
    {
        internal static JournalOperationTraceContext ForKey(IJournalCoordinator coordinator, CacheKey key, int? payloadBytes = null) => new()
        {
            Namespace = string.IsNullOrEmpty(key.Namespace) ? null : key.Namespace,
            PayloadBytes = payloadBytes,
            GroupCommitEnabled = coordinator.IsJournalGroupCommitEnabled,
        };

        internal static JournalOperationTraceContext? WithDurability(IJournalCoordinator coordinator, in JournalOperationTraceContext? context) => context switch
        {
            { } ctx => ctx with
            {
                GroupCommitEnabled = coordinator.IsJournalGroupCommitEnabled,
            },
            _ => null,
        };
    }
}
