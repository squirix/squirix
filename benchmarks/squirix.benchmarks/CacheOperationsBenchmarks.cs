using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Phase-3 remote cache operation benchmarks over a single long-lived client session.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class CacheOperationsBenchmarks : RemoteBenchmarkLifecycleBase
{
    private const int CheapBatch = 10_000;
    private const string ExistingKey = "bench_existing";
    private const int LightBatch = 10_000;
    private const string MissingKey = "bench_missing";

    /// <summary>
    /// Measures single-key <c>AddAsync</c> with a freshly generated key per call.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the add finishes.</returns>
    [Benchmark]
    public Task AddNewKey() => SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None);

    /// <summary>
    /// Measures existence checks against a deliberately missing key across many iterations.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all lookups finish.</returns>
    [Benchmark(OperationsPerInvoke = CheapBatch)]
    public async Task ContainsMissingBatched()
    {
        for (var i = 0; i < CheapBatch; i++)
            _ = await SharedCache.GetValueAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Ensure "missing" key is absent for negative-path benchmarks.</summary>
    [IterationSetup(Targets = [nameof(ContainsMissingBatched), nameof(RemoveMissingBatched)])]
    public void EnsureMissingAbsent() => _ = SharedCache.RemoveAsync(MissingKey, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>
    /// Measures repeated reads against a pre-seeded key.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when repeated reads finish.</returns>
    [Benchmark]
    public async Task GetExistingValue()
    {
        for (var i = 0; i < LightBatch; i++)
            _ = await SharedCache.GetValueAsync(ExistingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Batches lightweight <c>SetAsync</c> calls to amortize per-iteration BenchmarkDotNet overhead.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all inserts finish.</returns>
    [Benchmark(OperationsPerInvoke = LightBatch)]
    public async Task InsertNewKeyBatched()
    {
        for (var i = 0; i < LightBatch; i++)
            await SharedCache.SetAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Measures removals against keys that remain absent despite repeated optimistic deletes.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all remove attempts finish.</returns>
    [Benchmark(OperationsPerInvoke = CheapBatch)]
    public async Task RemoveMissingBatched()
    {
        for (var i = 0; i < CheapBatch; i++)
            _ = await SharedCache.RemoveAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens the node, client, cache session, and seeds baseline keys.
    /// </summary>
    [GlobalSetup]
    public void StartCacheSession()
    {
        StartNode();
        StartSharedCache("bench");
        SharedCache.SetAsync(ExistingKey, "value", cancellationToken: CancellationToken.None).GetAwaiter().GetResult();
        _ = SharedCache.RemoveAsync(MissingKey, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Disposes the cache session and stops the benchmark node.
    /// </summary>
    [GlobalCleanup]
    public void StopCacheSession()
    {
        StopSharedCache();
        StopNode();
    }
}
