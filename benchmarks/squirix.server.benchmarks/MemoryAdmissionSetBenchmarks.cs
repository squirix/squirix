using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of a Set through memory admission over a stored-entry read that holds an entry, without pressure.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class MemoryAdmissionSetBenchmarks
{
    private const string CacheName = "cache";
    private const string Key = "key";
    private const string NodeId = "n1";
    private const int OperationsPerInvoke = 100_000;
    private MemoryAdmissionCacheDecorator<object?>? _admission;
    private Meter? _meter;

    /// <summary>Releases the meter created during setup.</summary>
    [GlobalCleanup]
    public void Cleanup() => _meter?.Dispose();

    /// <summary>Creates the admission decorator over an inner cache that completes synchronously.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("Squirix.Benchmarks.AdmissionSet");
        var estimator = new CacheEntrySizeEstimator<object?>();
        var accounting = new MemoryUsageAccounting(new PhysicalCache<object?>(TimeProvider.System, estimator));
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), accounting, NodeId, _meter);
        _admission = new MemoryAdmissionCacheDecorator<object?>(new StoredCache(), gate, estimator);
    }

    /// <summary>Runs Sets that replace a stored entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task SetAsync()
    {
        var admission = ThrowHelper.Required(_admission, "Benchmark decorator was not initialized.");
        var entry = new NodeCacheEntry<object?>("value");
        for (var i = 0; i < OperationsPerInvoke; i++)
            await admission.SetEntryAsync("op", CacheName, Key, entry, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Inner cache that holds one entry and completes every call at once.</summary>
    [Immutable]
    private sealed class StoredCache : ILogicalNamespacedCache<object?>
    {
        private readonly NodeCacheEntry<object?> _stored = new("stored");

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<object?>?>(_stored);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
