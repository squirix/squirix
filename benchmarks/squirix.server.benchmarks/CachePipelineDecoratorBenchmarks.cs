using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Allocation cost of the cache pipeline decorators from tracing down to memory admission, over an inner cache whose writes complete on another thread
/// the way a durable or replicated write does, and whose reads complete at once.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class CachePipelineDecoratorBenchmarks
{
    private const string CacheName = "cache";
    private const string Key = "key";
    private const string NodeId = "n1";
    private const int OperationsPerInvoke = 20_000;
    private AdmissionGate? _gate;
    private Meter? _meter;
    private Meter? _metricsMeter;
    private TracingCacheDecorator<object?>? _pipeline;

    /// <summary>Releases the gate and meters created during setup.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _gate?.Dispose();
        _meter?.Dispose();
        _metricsMeter?.Dispose();
    }

    /// <summary>Runs reads that complete synchronously through the whole chain.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ReadAsync()
    {
        var pipeline = ThrowHelper.Required(_pipeline, "Benchmark pipeline was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await pipeline.GetEntryAsync(CacheName, Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Creates the decorator chain over the suspending inner cache.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("Squirix.Benchmarks.PipelineAdmission");
        _metricsMeter = new Meter("Squirix.Benchmarks.PipelineMetrics");
        _gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(_meter), TimeProvider.System);
        var estimator = new CacheEntrySizeEstimator<object?>();
        var accounting = new MemoryUsageAccounting(new PhysicalCache<object?>(TimeProvider.System, estimator));
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var pressureGate = new PressureGate(new StateEvaluator(options), accounting, NodeId, _meter);
        var admission = new MemoryAdmissionCacheDecorator<object?>(new SuspendingCache(), pressureGate, estimator);
        var metrics = new MetricsCacheDecorator<object?>(admission, new CacheMetrics(_metricsMeter));
        var backpressure = new BackpressureCacheDecorator<object?>(metrics, _gate, SharedClientIdResolver.Instance);
        var ownership = new OwnershipGuardCacheDecorator<object?>(NodeId, new SelfOwner(), new SelfLeader(), false, backpressure);
        var validation = new ValidationCacheDecorator<object?>(ownership);
        var mapping = new DomainErrorMappingCacheDecorator<object?>(validation);
        _pipeline = new TracingCacheDecorator<object?>(mapping, NodeId);
    }

    /// <summary>Runs writes that suspend in the inner cache and resume on another thread.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task SuspendingSetAsync()
    {
        var pipeline = ThrowHelper.Required(_pipeline, "Benchmark pipeline was not initialized.");
        var entry = new NodeCacheEntry<object?>("value");
        for (var i = 0; i < OperationsPerInvoke; i++)
            await pipeline.SetEntryAsync("op", CacheName, Key, entry, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Runs conditional adds that suspend in the inner cache and resume on another thread.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task SuspendingTryAddAsync()
    {
        var pipeline = ThrowHelper.Required(_pipeline, "Benchmark pipeline was not initialized.");
        var entry = new NodeCacheEntry<object?>("value");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await pipeline.TryAddEntryAsync("op", CacheName, Key, entry, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Owner lookup that names this node for every key.</summary>
    [Immutable]
    private sealed class SelfOwner : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => NodeId;
    }

    /// <summary>Leader table in which this node leads every group with authority.</summary>
    [Immutable]
    private sealed class SelfLeader : IGroupLeaderTable
    {
        public bool HasLocalAuthority(string groupId, out ulong term)
        {
            term = 1;
            return true;
        }

        public void Learn(string groupId, in LeaderRoute hint) => throw new NotSupportedException();

        public GroupLeaderView Read(string groupId) => new(true, true, false, 1, 1, new LeaderRoute(NodeId, 1));

        public void Refute(string groupId, in LeaderRoute route) => throw new NotSupportedException();

        public bool TryGetLeader(string groupId, out LeaderRoute route) => throw new NotSupportedException();

        public bool TryGetLearnedLeader(string groupId, out LeaderRoute route) => throw new NotSupportedException();

        public ValueTask<bool> WaitForAuthorityAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<LeaderRoute> WaitForLeaderChangeAsync(string groupId, LeaderRoute previous, TimeSpan timeout, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    /// <summary>Inner cache whose writes stay pending until a thread pool thread completes them, without allocating per operation.</summary>
    private sealed class SuspendingCache : ILogicalNamespacedCache<object?>
    {
        private readonly PendingCompletion _completion = new();

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<object?>?>(null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _completion.BeginAsync();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _completion.BeginBoolAsync();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    /// <summary>One reusable pending operation that completes from a thread pool work item once a continuation is registered.</summary>
    private sealed class PendingCompletion : IValueTaskSource, IValueTaskSource<bool>, IThreadPoolWorkItem
    {
        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };

        public void Execute() => _core.SetResult(true);

        void IValueTaskSource.GetResult(short token) => _ = Finish(token);

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        bool IValueTaskSource<bool>.GetResult(short token) => Finish(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _core.OnCompleted(continuation, state, token, flags);
            _ = ThreadPool.UnsafeQueueUserWorkItem(this, false);
        }

        internal ValueTask BeginAsync() => new(this, _core.Version);

        internal ValueTask<bool> BeginBoolAsync() => new(this, _core.Version);

        private bool Finish(short token)
        {
            var result = _core.GetResult(token);
            _core.Reset();
            return result;
        }
    }
}
