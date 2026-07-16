using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Support.Cluster;

namespace Squirix.Benchmarks.Client;

/// <summary>Phase-2 remote client benchmark: acquire a cache handle on an existing connection.</summary>
[MemoryDiagnoser]
public class CacheHandleBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>Measures cache handle acquisition after connect.</summary>
    /// <returns>A task that completes after the cache handle is acquired.</returns>
    [Benchmark]
    [InvocationCount(1)]
    public Task GetCacheHandleAsync() => GetCacheHandleAndDisposeAsync("bench-handle");

    /// <summary>Starts the benchmark node.</summary>
    /// <returns>A task that completes after the node is started.</returns>
    [GlobalSetup]
    public Task SetupBenchmarkAsync() => StartNodeAsync();

    /// <summary>Stops the benchmark node.</summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    [GlobalCleanup]
    public Task TeardownBenchmarkAsync() => StopNodeAsync();
}
