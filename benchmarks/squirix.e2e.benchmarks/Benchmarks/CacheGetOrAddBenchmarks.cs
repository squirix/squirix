using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// End-to-end public API benchmarks for GetOrAddAsync.
/// </summary>
[BenchmarkCategory("e2e", "get-or-add")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet requires instance benchmark members.")]
public class CacheGetOrAddBenchmarks : CacheBenchmarkBase
{
    /// <summary>
    /// Measures GetOrAddAsync hit path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("get-or-add", "read")]
    public async Task GetOrAddShouldReturnExistingValue()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetOrAddHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures GetOrAddAsync miss and create path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("get-or-add", "write")]
    public async Task GetOrAddShouldCreateMissingValue()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetOrAddMissAsync(NextAddKey(), i, CancellationToken.None).ConfigureAwait(false));
    }
}
