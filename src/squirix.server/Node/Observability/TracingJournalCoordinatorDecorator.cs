using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Observability;

/// <summary>Adds OpenTelemetry spans around journal coordinator operations.</summary>
internal sealed class TracingJournalCoordinatorDecorator : IJournalCoordinator
{
    private readonly EventHandler _forwardOnAppended;
    private readonly IJournalCoordinator _inner;
    private readonly IJournalOperationTracer _tracer;

    internal TracingJournalCoordinatorDecorator(IJournalCoordinator inner, IJournalOperationTracer tracer)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
        _forwardOnAppended = ForwardOnAppended;
        _inner.OnAppended += _forwardOnAppended;
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => _inner.AppendedBytes;

    public long AppendedOps => _inner.AppendedOps;

    public int CurrentSegmentIndex => _inner.CurrentSegmentIndex;

    public long HighWaterBytes => _inner.HighWaterBytes;

    public bool HasFlushLoopFailure => _inner.HasFlushLoopFailure;

    public bool IsJournalGroupCommitEnabled => _inner.IsJournalGroupCommitEnabled;

    public long MaxBytes => _inner.MaxBytes;

    public ulong NextSequence => _inner.NextSequence;

    public double RecentAppendLatencyMs => _inner.RecentAppendLatencyMs;

    public long UsedBytes => _inner.UsedBytes;

    public async ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.IdempotencyOutcome, in traceContext);
        await _inner.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        var payloadBytes = entryBytes.Length;
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key) with { PayloadBytes = payloadBytes });
        using var scope = _tracer.Begin(JournalOperationKind.Put, in traceContext);
        await _inner.AppendPutAndAwaitDurabilityAsync(key, entryBytes, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        var payloadBytes = entryBytes.Length;
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key) with { PayloadBytes = payloadBytes });
        using var scope = _tracer.Begin(JournalOperationKind.Put, in traceContext);
        await _inner.AppendPutAsync(key, entryBytes, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key));
        using var scope = _tracer.Begin(JournalOperationKind.Remove, in traceContext);
        await _inner.AppendRemoveAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key));
        using var scope = _tracer.Begin(JournalOperationKind.RemoveExpiration, in traceContext);
        await _inner.AppendRemoveExpirationAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key));
        using var scope = _tracer.Begin(JournalOperationKind.TouchExpiration, in traceContext);
        await _inner.AppendTouchExpirationAsync(key, expiresUtc, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.AwaitDurabilityCommit, in traceContext);
        await _inner.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public void BeginPendingMemoryApply() => _inner.BeginPendingMemoryApply();

    public void CompletePendingMemoryApply() => _inner.CompletePendingMemoryApply();

    public ValueTask DisposeAsync()
    {
        _inner.OnAppended -= _forwardOnAppended;
        return _inner.DisposeAsync();
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.MaintenanceExclusive, in traceContext);
        await _inner.ExecuteMaintenanceExclusiveAsync(action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.SnapshotCut, in traceContext);
        return await _inner.ExecuteSnapshotCutAsync(state, captureUnderBarrier, buildOutsideBarrier, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.UnderSnapshotBarrier, in traceContext);
        return await _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.UnderSnapshotBarrier, in traceContext);
        return await _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask WaitForStartupAsync(CancellationToken cancellationToken)
    {
        var traceContext = Enrich(null);
        using var scope = _tracer.Begin(JournalOperationKind.WaitForStartup, in traceContext);
        await _inner.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);
    }

    private JournalOperationTraceContext? Enrich(JournalOperationTraceContext? context) => JournalCoordinatorTracing.WithDurability(in context, _inner);

    private void ForwardOnAppended(object? sender, EventArgs e) => OnAppended?.Invoke(this, e);

    /// <summary>
    /// Helpers for tracing journal coordinator operations through <see cref="IJournalOperationTracer" />.
    /// </summary>
    private static class JournalCoordinatorTracing
    {
        internal static JournalOperationTraceContext ForKey(CacheKey key) => new()
        {
            Key = key.Key,
            Namespace = string.IsNullOrEmpty(key.Namespace) ? null : key.Namespace,
        };

        internal static JournalOperationTraceContext? WithDurability(in JournalOperationTraceContext? context, IJournalCoordinator coordinator)
        {
            if (context != null)
            {
                return context with
                {
                    GroupCommitEnabled = coordinator.IsJournalGroupCommitEnabled,
                };
            }

            return null;
        }
    }
}
