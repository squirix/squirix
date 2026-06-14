using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// End-to-end public API benchmarks for deterministic mixed workloads.
/// </summary>
[BenchmarkCategory("e2e", "mixed")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet requires instance benchmark members.")]
public class CacheMixedWorkloadBenchmarks : CacheBenchmarkBase
{
    /// <summary>
    /// Measures a hot-key read-mostly workload.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mixed", "hot-keys", "read")]
    public async Task HotKeyReadMostlyShouldExecuteAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            if (i % 20 == 0)
                await Adapter.SetAsync(Keyspace.HotKey(i), i, CancellationToken.None).ConfigureAwait(false);
            else
                Consumer.Consume(await Adapter.GetValueHitAsync(Keyspace.HotKey(i), CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>
    /// Measures a 95 percent read, 5 percent write workload.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mixed", "read", "write")]
    public async Task ReadHeavy95To5ShouldExecuteAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            if (i % 20 == 0)
                await Adapter.SetAsync(NextAddKey(), i, CancellationToken.None).ConfigureAwait(false);
            else
                Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>
    /// Measures an 80 percent read, 15 percent write, 5 percent remove workload.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mixed", "read", "write", "mutation")]
    public async Task ReadMostly80To15To5ShouldExecuteAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            if (i % 20 == 0)
            {
                Consumer.Consume(await Adapter.RemoveAsync(NextMissKey(), CancellationToken.None).ConfigureAwait(false));
            }
            else if (i % 7 == 0 || i % 11 == 0 || i % 13 == 0)
            {
                await Adapter.SetAsync(NextAddKey(), i, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
            }
        }
    }

    /// <summary>
    /// Measures a read-mostly workload against remote-owner key selection when the scenario uses it.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mixed", "remote-owner", "read")]
    public async Task RemoteOwnerReadMostlyShouldExecuteAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            if (i % 20 == 0)
                Consumer.Consume(await Adapter.UpdateAsync(NextHitKey(), i, CancellationToken.None).ConfigureAwait(false));
            else
                Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>
    /// Measures a read-mostly workload over the scenario key distribution.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("mixed", "uniform-keys", "read")]
    public async Task UniformTwoNodeReadMostlyShouldExecuteAsync()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            if (i % 20 == 0)
                await Adapter.SetAsync(NextAddKey(), i, CancellationToken.None).ConfigureAwait(false);
            else
                Consumer.Consume(await Adapter.GetValueHitAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
        }
    }
}
