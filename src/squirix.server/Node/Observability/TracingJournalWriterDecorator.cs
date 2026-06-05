using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Adds OpenTelemetry spans around journal coordinator operations.
/// </summary>
internal sealed class TracingJournalWriterDecorator : IJournalCoordinator
{
    private readonly JournalWriter _inner;
    private readonly IJournalOperationTracer _tracer;

    public TracingJournalWriterDecorator(JournalWriter inner, IJournalOperationTracer tracer)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
    }

    public event Action? OnAppended
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

    public ValueTask AppendPutAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken) =>
        TracePutAsync(key.Key, key.Namespace, discriminatedEntryJson, operationId, cancellationToken);

    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.RemoveExpiration,
        Enrich(JournalWriterTracing.ForKey(key)),
        () => _inner.AppendRemoveExpirationAsync(key, cancellationToken));

    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.Remove,
        Enrich(JournalWriterTracing.ForKey(key)),
        () => _inner.AppendRemoveAsync(key, cancellationToken));

    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.TouchExpiration,
        Enrich(JournalWriterTracing.ForKey(key)),
        () => _inner.AppendTouchExpirationAsync(key, expiresUtc, cancellationToken));

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.AwaitDurabilityCommit,
        Enrich(default),
        () => _inner.AwaitDurabilityCommitAsync(cancellationToken));

    public void BeginPendingMemoryApply() => _inner.BeginPendingMemoryApply();

    public void CompletePendingMemoryApply() => _inner.CompletePendingMemoryApply();

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.MaintenanceExclusive,
        Enrich(default),
        () => _inner.ExecuteMaintenanceExclusiveAsync(action, cancellationToken));

    public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.SnapshotCut,
        Enrich(default),
        () => _inner.ExecuteSnapshotCutAsync(state, action, cancellationToken));

    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
        JournalWriterTracing.TraceAsync(
            _tracer,
            JournalOperationKind.UnderSnapshotBarrier,
            Enrich(default),
            () => _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken));

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => JournalWriterTracing.TraceAsync(
        _tracer,
        JournalOperationKind.WaitForStartup,
        Enrich(default),
        () => _inner.WaitForStartupAsync(cancellationToken));

    private JournalOperationTraceContext Enrich(in JournalOperationTraceContext context) => JournalWriterTracing.WithDurability(in context, _inner);

    private async ValueTask TracePutAsync(string key, string cacheNamespace, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        var payloadBytes = discriminatedEntryJson.Length;
        var context = Enrich(JournalWriterTracing.ForKey(key, cacheNamespace) with { PayloadBytes = payloadBytes });
        await JournalWriterTracing.TraceAsync(
            _tracer,
            JournalOperationKind.Put,
            context,
            () => _inner.AppendPutAsync(new CacheKey(cacheNamespace, key), discriminatedEntryJson, operationId, cancellationToken),
            scope => JournalWriterTracing.TraceFrameBytes(scope, payloadBytes)).ConfigureAwait(false);
    }
}
