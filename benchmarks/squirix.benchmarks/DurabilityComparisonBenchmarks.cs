using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Compares client SDK throughput with ephemeral and persistent server modes.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class DurabilityComparisonBenchmarks
{
    private const int Batch = 1_024;
    private const string CacheName = "durability-comparison";
    private const string ExistingKey = "bench_existing";

    private BenchmarkCacheSession? _cacheSession;
    private BenchmarkNodeScope? _node;

    /// <summary>
    /// Gets or sets the durability mode measured by the current BenchmarkDotNet case.
    /// </summary>
    [Params(BenchmarkDurabilityMode.Ephemeral, BenchmarkDurabilityMode.Persistence)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Property annotated with [Params] must have a public setter")]
    public BenchmarkDurabilityMode DurabilityMode { get; set; }

    private ICache<object?> SharedCache => (_cacheSession ?? throw new InvalidOperationException("Shared cache session was not opened.")).Cache;

    /// <summary>
    /// Measures single-key <c>AddAsync</c> with a freshly generated key per call.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the add finishes.</returns>
    [Benchmark]
    public Task AddNewKeyAsync() => SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None);

    /// <summary>
    /// Measures batched reads of an existing key.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch finishes.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task GetExistingBatchedAsync()
    {
        for (var i = 0; i < Batch; i++)
            _ = await SharedCache.GetValueAsync(ExistingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the benchmark node and shared cache session.
    /// </summary>
    /// <returns>A task that completes after benchmark resources are disposed.</returns>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_cacheSession is not null)
            await _cacheSession.DisposeAsync().ConfigureAwait(false);
        _cacheSession = null;
        if (_node is not null)
            await _node.DisposeAsync().ConfigureAwait(false);
        _node = null;
    }

    /// <summary>
    /// Starts the benchmark node and opens a shared cache session.
    /// </summary>
    /// <returns>A task that completes after benchmark resources are ready.</returns>
    [GlobalSetup]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to fields disposed in GlobalCleanup.")]
    public async Task GlobalSetupAsync()
    {
        BenchmarkRuntime.EnsureInitialized();
        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None, DurabilityMode).ConfigureAwait(false);
        _cacheSession = await BenchmarkCacheSession.OpenAsync(_node, CacheName, CancellationToken.None).ConfigureAwait(false);
        await SharedCache.AddAsync(ExistingKey, "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Measures batched inserts of new keys.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch finishes.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task InsertNewKeyBatchedAsync()
    {
        for (var i = 0; i < Batch; i++)
            await SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
