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
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
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
    public Task AddNewKeyAsync() => SharedCache.AddAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None);

    /// <summary>
    /// Measures existence checks against a deliberately missing key across many iterations.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all lookups finish.</returns>
    [Benchmark(OperationsPerInvoke = CheapBatch)]
    public async Task ContainsMissingBatchedAsync()
    {
        for (var i = 0; i < CheapBatch; i++)
            _ = await SharedCache.GetValueAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Ensure "missing" key is absent for negative-path benchmarks.</summary>
    /// <returns>A task that completes after the missing key is removed.</returns>
    [IterationSetup(Targets = [nameof(ContainsMissingBatchedAsync), nameof(RemoveMissingBatchedAsync)])]
    public async Task EnsureMissingAbsentAsync() => _ = await SharedCache.RemoveAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);

    /// <summary>
    /// Measures repeated reads against a pre-seeded key.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when repeated reads finish.</returns>
    [Benchmark]
    public async Task GetExistingValueAsync()
    {
        for (var i = 0; i < LightBatch; i++)
            _ = await SharedCache.GetValueAsync(ExistingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Batches lightweight <c>SetAsync</c> calls to amortize per-iteration BenchmarkDotNet overhead.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all inserts finish.</returns>
    [Benchmark(OperationsPerInvoke = LightBatch)]
    public async Task InsertNewKeyBatchedAsync()
    {
        for (var i = 0; i < LightBatch; i++)
            await SharedCache.SetAsync(Guid.NewGuid().ToString("N"), "v", cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Measures removals against keys that remain absent despite repeated optimistic deletes.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when all remove attempts finish.</returns>
    [Benchmark(OperationsPerInvoke = CheapBatch)]
    public async Task RemoveMissingBatchedAsync()
    {
        for (var i = 0; i < CheapBatch; i++)
            _ = await SharedCache.RemoveAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens the node, client, cache session, and seeds baseline keys.
    /// </summary>
    /// <returns>A task that completes after benchmark resources are ready.</returns>
    [GlobalSetup]
    public async Task StartCacheSessionAsync()
    {
        await StartNodeAsync().ConfigureAwait(false);
        await StartSharedCacheAsync("bench").ConfigureAwait(false);
        await SharedCache.SetAsync(ExistingKey, "value", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        _ = await SharedCache.RemoveAsync(MissingKey, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes the cache session and stops the benchmark node.
    /// </summary>
    /// <returns>A task that completes after benchmark resources are disposed.</returns>
    [GlobalCleanup]
    public async Task StopCacheSessionAsync()
    {
        await StopSharedCacheAsync().ConfigureAwait(false);
        await StopNodeAsync().ConfigureAwait(false);
    }
}
