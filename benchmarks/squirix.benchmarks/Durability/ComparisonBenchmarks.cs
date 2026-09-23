using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Support;
using Squirix.Benchmarks.Support.Client;
using Squirix.Benchmarks.Support.Cluster;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Benchmarks.Durability;

/// <summary>Compares client SDK throughput with ephemeral and persistent server modes.</summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class ComparisonBenchmarks
{
    private const int Batch = 1_024;
    private const string CacheName = "durability-comparison";
    private const string ExistingKey = "bench_existing";

    private BenchmarkCacheSession? _cacheSession;
    private TestCluster<ClusterStartOptions>? _cluster;

    /// <summary>Gets or sets the durability mode measured by the current BenchmarkDotNet case.</summary>
    [Params(BenchmarkDurabilityMode.Ephemeral, BenchmarkDurabilityMode.Persistence)]
    public BenchmarkDurabilityMode DurabilityMode { get; set; }

    private ICache<object?> SharedCache => BenchmarkThrowHelper.Required(_cacheSession, "Shared cache session was not opened.").Cache;

    /// <summary>Measures single-key <c language="csharp">AddAsync</c> with a freshly generated key per call.</summary>
    [Benchmark]
    public Task AddNewKeyAsync() => SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None);

    /// <summary>Measures batched reads of an existing key.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetExistingBatchedAsync()
    {
        for (var i = 0; i < Batch; i++)
            _ = await SharedCache.GetValueAsync(ExistingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stops the benchmark node and shared cache session.</summary>
    /// <returns>A task that completes after benchmark resources are disposed.</returns>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_cacheSession != null)
            await _cacheSession.DisposeAsync().ConfigureAwait(false);
        _cacheSession = null;
        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
        _cluster = null;
    }

    /// <summary>Starts the benchmark node and opens a shared cache session.</summary>
    /// <returns>A task that completes after benchmark resources are ready.</returns>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _cluster = await BenchmarkNodeCluster.StartAsync(DurabilityMode, CancellationToken.None).ConfigureAwait(false);
        _cacheSession = await BenchmarkCacheSession.OpenAsync(_cluster.Uri(), CacheName, CancellationToken.None).ConfigureAwait(false);
        await SharedCache.AddAsync(ExistingKey, "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Measures batched inserts of new keys.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task InsertNewKeyBatchedAsync()
    {
        for (var i = 0; i < Batch; i++)
            await SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
