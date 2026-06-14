using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Phase-2 remote client benchmark: acquire a cache handle on an existing connection.
/// </summary>
[MemoryDiagnoser]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class ClientCacheHandleBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>
    /// Measures cache handle acquisition after connect.
    /// </summary>
    /// <returns>A task that completes after the cache handle is acquired.</returns>
    [Benchmark]
    [InvocationCount(1)]
    public async Task GetCacheHandleAsync() => await GetCacheHandleAndDisposeAsync("bench-handle").ConfigureAwait(false);

    /// <summary>Starts the benchmark node.</summary>
    /// <returns>A task that completes after the node is started.</returns>
    [GlobalSetup]
    public async Task SetupBenchmarkAsync() => await StartNodeAsync().ConfigureAwait(false);

    /// <summary>Stops the benchmark node.</summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    [GlobalCleanup]
    public async Task TeardownBenchmarkAsync() => await StopNodeAsync().ConfigureAwait(false);
}
