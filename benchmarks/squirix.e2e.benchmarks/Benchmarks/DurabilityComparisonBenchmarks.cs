using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Scenarios;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// Focused durability comparison benchmarks on a fixed single-node scenario.
/// </summary>
[BenchmarkCategory("e2e", "durability")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet requires instance benchmark members.")]
public class DurabilityComparisonBenchmarks : CacheBenchmarkBase
{
    /// <inheritdoc />
    public override IEnumerable<BenchmarkScenario> Scenarios => BenchmarkScenario.CreateDurabilityComparisonMatrix();

    /// <summary>
    /// Measures GetValueAsync hit throughput.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("read")]
    public async Task GetValueShouldReturnHit()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures SetAsync throughput for new keys.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("write")]
    public async Task SetShouldStoreValue()
    {
        for (var i = 0; i < BatchSize; i++)
            await Adapter.SetAsync(NextUniqueAddKey(), i, CancellationToken.None).ConfigureAwait(false);
    }
}
