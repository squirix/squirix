using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of the journal and cache tracing decorators over cheap inner doubles when no trace listener is attached.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class TracingDecoratorBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private const string CacheName = "bench";
    private const string KeyName = "key";
    private static readonly CacheKey Key = new(CacheName, KeyName);
    private static readonly byte[] Payload = new byte[256];
    private readonly TracingCacheDecorator<string> _cache = new(new CompletedCache(), "node");
    private readonly TracingJournalCoordinatorDecorator _asyncJournal = new(new JournalDouble(true), new NoListenerTracer());
    private readonly TracingJournalCoordinatorDecorator _completedJournal = new(new JournalDouble(false), new NoListenerTracer());

    /// <summary>Appends puts through the tracing decorator over an inner append that completes asynchronously.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AppendPutAsynchronousAsync()
    {
        for (var i = 0; i < OperationsPerInvoke; i++)
            await _asyncJournal.AppendPutAsync(default, Key, Payload, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Appends puts through the tracing decorator over an inner append that completes synchronously.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AppendPutSynchronousAsync()
    {
        for (var i = 0; i < OperationsPerInvoke; i++)
            await _completedJournal.AppendPutAsync(default, Key, Payload, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads values through the tracing cache decorator over an inner cache that completes synchronously.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task GetValueAsync()
    {
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await _cache.GetValueAsync(CacheName, KeyName, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Tracer that reports tracing as disabled, like a production node without a listener.</summary>
    private sealed class NoListenerTracer : IJournalOperationTracer
    {
        bool IJournalOperationTracer.IsEnabled => false;

        IJournalOperationTraceScope? IJournalOperationTracer.Begin(JournalOperationKind kind, in JournalOperationTraceContext? context) => null;
    }

    /// <summary>Cache whose value reads complete synchronously.</summary>
    private sealed class CompletedCache : ILogicalNamespacedCache<string>
    {
        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<string>(true, "v"));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    /// <summary>Journal coordinator whose appends do nothing and complete either synchronously or after a yield.</summary>
    private sealed class JournalDouble : IJournalCoordinator
    {
        private readonly bool _yields;

        internal JournalDouble(bool yields)
        {
            _yields = yields;
        }

        public event EventHandler? OnAppended
        {
            add => _ = value;
            remove => _ = value;
        }

        public long AppendedBytes => 0;

        public long AppendedOps => 0;

        public bool AtHighWater => false;

        public int CurrentSegmentIndex => 0;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate => throw new NotSupportedException();

        public bool IsJournalGroupCommitEnabled => false;

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, Action? appended, CancellationToken cancellationToken) => CompleteAsync();

        public ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => CompleteAsync();

        public ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken) => CompleteAsync();

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => CompleteAsync();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(
            Func<AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public void FailJournalPipeline(Exception reason) => throw new NotSupportedException();

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => CompleteAsync();

        private static ValueTask CompleteOnPoolAsync()
        {
            var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = ThreadPool.UnsafeQueueUserWorkItem(static pending => pending.SetResult(), source, false);
            return new ValueTask(source.Task);
        }

        private ValueTask CompleteAsync() => _yields ? CompleteOnPoolAsync() : ValueTask.CompletedTask;
    }
}
