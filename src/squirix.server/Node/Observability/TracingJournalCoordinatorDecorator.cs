using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.Node.Observability;

/// <summary>Adds OpenTelemetry spans around journal coordinator operations.</summary>
internal sealed class TracingJournalCoordinatorDecorator : IJournalCoordinator
{
    private readonly IJournalCoordinator _inner;
    private readonly IJournalOperationTracer _tracer;

    public TracingJournalCoordinatorDecorator(IJournalCoordinator inner, IJournalOperationTracer tracer)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
    }

    public event EventHandler? OnAppended
    {
        add => _inner.OnAppended += value;
        remove => _inner.OnAppended -= value;
    }

    public long AppendedBytes => _inner.AppendedBytes;

    public long AppendedOps => _inner.AppendedOps;

    public int CurrentSegmentIndex => _inner.CurrentSegmentIndex;

    public bool HasFlushLoopFailure => _inner.HasFlushLoopFailure;

    public bool IsJournalGroupCommitEnabled => _inner.IsJournalGroupCommitEnabled;

    public ulong NextSequence => _inner.NextSequence;

    public double RecentAppendLatencyMs => _inner.RecentAppendLatencyMs;

    public async ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, string? operationId, CancellationToken cancellationToken)
    {
        var payloadBytes = entryBytes.Length;
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key) with { PayloadBytes = payloadBytes });
        using var scope = _tracer.Begin(JournalOperationKind.Put, in traceContext);
        await _inner.AppendPutAndAwaitDurabilityAsync(key, entryBytes, operationId, cancellationToken).ConfigureAwait(false);
        JournalCoordinatorTracing.TraceFrameBytes(scope, payloadBytes);
    }

    public async ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, string? operationId, CancellationToken cancellationToken)
    {
        var payloadBytes = entryBytes.Length;
        var traceContext = Enrich(JournalCoordinatorTracing.ForKey(key) with { PayloadBytes = payloadBytes });
        using var scope = _tracer.Begin(JournalOperationKind.Put, in traceContext);
        await _inner.AppendPutAsync(key, entryBytes, operationId, cancellationToken).ConfigureAwait(false);
        JournalCoordinatorTracing.TraceFrameBytes(scope, payloadBytes);
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
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.AwaitDurabilityCommit, in traceContext);
        await _inner.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public void BeginPendingMemoryApply() => _inner.BeginPendingMemoryApply();

    public void CompletePendingMemoryApply() => _inner.CompletePendingMemoryApply();

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.MaintenanceExclusive, in traceContext);
        await _inner.ExecuteMaintenanceExclusiveAsync(action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken)
    {
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.SnapshotCut, in traceContext);
        return await _inner.ExecuteSnapshotCutAsync(state, captureUnderBarrier, buildOutsideBarrier, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.UnderSnapshotBarrier, in traceContext);
        return await _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken)
    {
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.UnderSnapshotBarrier, in traceContext);
        return await _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask WaitForStartupAsync(CancellationToken cancellationToken)
    {
        var traceContext = Enrich(default);
        using var scope = _tracer.Begin(JournalOperationKind.WaitForStartup, in traceContext);
        await _inner.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);
    }

    private JournalOperationTraceContext Enrich(JournalOperationTraceContext context) => JournalCoordinatorTracing.WithDurability(in context, _inner);
}
