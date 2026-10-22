using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Scaling of <see cref="PhysicalCache{T}" /> reads and writes with concurrent workers over keys that already exist. The same worker harness
/// over a <see cref="ConcurrentDictionary{TKey, TValue}" /> shows the ceiling without the cache, so the cost of its single gate can be told
/// from the cost of the harness. Each invocation runs the operations of every worker, so the mean per operation is the wall time of one
/// worker operation: flat across worker counts means perfect scaling. The product cache is unbounded; one bounded LRU case shows the cost
/// of the eviction order bookkeeping on reads.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(ContentionBenchmarkConfig))]
public class PhysicalCacheContentionBenchmarks
{
    private const int OperationsPerWorker = 65536;
    private const int WriteEvery = 20;
    private const int WorkerStride = 104729;
    private const int KeyStride = 7919;
    private static readonly TimeSpan EntryTtl = TimeSpan.FromDays(1);
    private static readonly NodeCacheEntry<object?> Template = new("v", 1, null, EntryTtl);
    private ConcurrentDictionary<CacheKey, NodeCacheEntry<object?>> _baseline = new();
    private PhysicalCache<object?>? _bounded;
    private PhysicalCache<object?>? _cache;
    private CacheKey[] _keys = [];

    private enum WorkerMode
    {
        Baseline = 0,
        Unbounded = 1,
        BoundedLru = 2,
    }

    /// <summary>Gets the worker counts: 1, 4 when the machine has more processors, and the processor count.</summary>
    public static IEnumerable<int> WorkerCounts
    {
        get
        {
            var counts = new List<int> { 1 };
            if (Environment.ProcessorCount > 4)
                counts.Add(4);
            if (Environment.ProcessorCount > 1)
                counts.Add(Environment.ProcessorCount);
            return counts;
        }
    }

    /// <summary>Gets or sets a value indicating whether committed records decide expiry in the measured cache; otherwise the local clock does.</summary>
    [Params(false, true)]
    public bool CommittedRecords { get; set; }

    /// <summary>Gets or sets the number of distinct keys the workers pick from.</summary>
    [Params(64, 100_000)]
    public int KeyCount { get; set; }

    /// <summary>Gets or sets a value indicating whether one operation in twenty is a write; otherwise every operation is a read.</summary>
    [Params(false, true)]
    public bool Mixed { get; set; }

    /// <summary>Gets or sets the number of concurrent workers.</summary>
    [ParamsSource(nameof(WorkerCounts))]
    public int Workers { get; set; }

    /// <summary>Runs the operations on a concurrent dictionary in place of the cache.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerWorker)]
    public Task<int> BaselineAsync() => RunWorkersAsync(WorkerMode.Baseline);

    /// <summary>Runs the operations on a bounded LRU cache whose capacity never evicts.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWorker)]
    public Task<int> BoundedLruAsync() => RunWorkersAsync(WorkerMode.BoundedLru);

    /// <summary>Creates the caches and the baseline, and stores every key.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _keys = new CacheKey[KeyCount];
        for (var i = 0; i < _keys.Length; i++)
            _keys[i] = CacheKey.Default(string.Create(CultureInfo.InvariantCulture, $"k{i}"));

        var expiry = CommittedRecords ? CacheExpiryAuthority.CommittedRecords : CacheExpiryAuthority.LocalClock;
        var estimator = new CacheEntrySizeEstimator<object?>();
        var cache = new PhysicalCache<object?>(TimeProvider.System, estimator, expiry: expiry);
        var bounded = new PhysicalCache<object?>(TimeProvider.System, estimator, new EvictionOptions { Capacity = KeyCount * 2, Policy = EvictionPolicyType.Lru }, expiry);
        _cache = cache;
        _bounded = bounded;
        _baseline = new ConcurrentDictionary<CacheKey, NodeCacheEntry<object?>>();
        foreach (var key in _keys)
        {
            await cache.SetAsync(key, Template, CancellationToken.None).ConfigureAwait(false);
            await bounded.SetAsync(key, Template, CancellationToken.None).ConfigureAwait(false);
            _baseline[key] = Template;
        }
    }

    /// <summary>Runs the operations on the unbounded cache, the shape the product uses.</summary>
    /// <returns>Operations completed, as a sink.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWorker)]
    public Task<int> UnboundedAsync() => RunWorkersAsync(WorkerMode.Unbounded);

    private int BaselineWorker(int worker)
    {
        var map = _baseline;
        var offset = worker * WorkerStride;
        var done = 0;
        for (var i = 0; i < OperationsPerWorker; i++)
        {
            var key = _keys[(offset + (i * KeyStride)) % _keys.Length];
            if (Mixed && i % WriteEvery == 0)
            {
                map[key] = new NodeCacheEntry<object?>("v", 1, null, EntryTtl);
                done++;
            }
            else if (map.ContainsKey(key))
            {
                done++;
            }
        }

        return done;
    }

    private async Task<int> CacheWorkerAsync(PhysicalCache<object?> cache, int worker)
    {
        var offset = worker * WorkerStride;
        var done = 0;
        for (var i = 0; i < OperationsPerWorker; i++)
        {
            var key = _keys[(offset + (i * KeyStride)) % _keys.Length];
            if (Mixed && i % WriteEvery == 0)
            {
                await cache.SetAsync(key, new NodeCacheEntry<object?>("v", 1, null, EntryTtl), CancellationToken.None).ConfigureAwait(false);
                done++;
            }
            else if (await cache.GetEntryAsync(key, CancellationToken.None).ConfigureAwait(false) != null)
            {
                done++;
            }
        }

        return done;
    }

    private Task<int> RunWorkerAsync(WorkerMode mode, int worker) => mode switch
    {
        WorkerMode.Baseline => Task.FromResult(BaselineWorker(worker)),
        WorkerMode.Unbounded => CacheWorkerAsync(ThrowHelper.Required(_cache, "Benchmark cache was not initialized."), worker),
        WorkerMode.BoundedLru => CacheWorkerAsync(ThrowHelper.Required(_bounded, "Benchmark cache was not initialized."), worker),
        _ => throw new InvalidOperationException("Unsupported worker mode."),
    };

    private async Task<int> RunWorkersAsync(WorkerMode mode)
    {
        var workers = Workers;
        var total = 0;
        using var ready = new CountdownEvent(workers);
        using var start = new ManualResetEventSlim(false);
        var tasks = new Task<int>[workers];
        for (var w = 0; w < workers; w++)
        {
            var run = new WorkerRun(this, mode, w, ready, start);
            tasks[w] = Task.Factory.StartNew(static state => Unsafe.As<WorkerRun>(state)!.ExecuteAsync(), run, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
        }

        ready.Wait(CancellationToken.None);
        start.Set();
        foreach (var task in tasks)
            total += await task.ConfigureAwait(false);

        return total;
    }

    private sealed class WorkerRun
    {
        private readonly WorkerMode _mode;
        private readonly PhysicalCacheContentionBenchmarks _owner;
        private readonly CountdownEvent _ready;
        private readonly ManualResetEventSlim _start;
        private readonly int _worker;

        internal WorkerRun(PhysicalCacheContentionBenchmarks owner, WorkerMode mode, int worker, CountdownEvent ready, ManualResetEventSlim start)
        {
            _owner = owner;
            _mode = mode;
            _worker = worker;
            _ready = ready;
            _start = start;
        }

        internal Task<int> ExecuteAsync()
        {
            _ = _ready.Signal();
            _start.Wait(CancellationToken.None);
            return _owner.RunWorkerAsync(_mode, _worker);
        }
    }
}
