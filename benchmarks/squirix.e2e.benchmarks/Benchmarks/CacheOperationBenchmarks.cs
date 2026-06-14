using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// End-to-end public API benchmarks for basic cache operations.
/// </summary>
[BenchmarkCategory("e2e", "read", "write", "mutation")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet requires instance benchmark members.")]
public class CacheOperationBenchmarks : CacheBenchmarkBase
{
    /// <summary>
    /// Measures AddAsync for missing keys.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write")]
    public async Task AddShouldStoreMissingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            await Adapter.AddAsync(NextUniqueAddKey(), i, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Measures AddAsync conflict exception path for existing keys.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write", "exception-path")]
    public async Task AddShouldThrowForExistingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.AddConflictAsync(NextHitKey(), i, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures GetEntryAsync hit path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("read")]
    public async Task GetEntryShouldReturnHitAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetEntryHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures GetValueAsync hit path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("read")]
    public async Task GetValueShouldReturnHitAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures GetValueAsync miss path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("read")]
    public async Task GetValueShouldReturnMissAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetValueMissAsync(NextMissKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures RemoveAsync for existing keys with inline reset.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mutation")]
    public async Task RemoveShouldDeleteExistingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            var key = NextAddKey();
            await Adapter.SetAsync(key, i, CancellationToken.None).ConfigureAwait(false);
            Consumer.Consume(await Adapter.RemoveAsync(key, CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>
    /// Measures RemoveAsync miss path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mutation")]
    public async Task RemoveShouldReturnFalseForMissingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.RemoveAsync(NextMissKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures SetAsync upsert path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write")]
    public async Task SetShouldStoreValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            await Adapter.SetAsync(NextAddKey(), i, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Measures TryAddAsync success path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write")]
    public async Task TryAddShouldAddMissingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.TryAddAsync(NextUniqueAddKey(), i, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures TryAddAsync conflict path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write")]
    public async Task TryAddShouldReturnFalseForExistingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(!await Adapter.TryAddAsync(NextHitKey(), i, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures UpdateAsync hit path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mutation")]
    public async Task UpdateShouldModifyExistingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.UpdateAsync(NextHitKey(), i, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures UpdateAsync miss path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mutation")]
    public async Task UpdateShouldReturnFalseForMissingValueAsync()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(!await Adapter.UpdateAsync(NextMissKey(), i, CancellationToken.None).ConfigureAwait(false));
    }
}
