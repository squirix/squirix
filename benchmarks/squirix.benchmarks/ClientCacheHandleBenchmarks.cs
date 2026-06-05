using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Phase-2 remote client benchmark: acquire a cache handle on an existing connection.
/// </summary>
[MemoryDiagnoser]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class ClientCacheHandleBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>
    /// Measures cache handle acquisition after connect.
    /// </summary>
    [Benchmark]
    [InvocationCount(1)]
    public void GetCacheHandle() => GetCacheHandleAndDispose("bench-handle");

    /// <summary>Starts the benchmark node.</summary>
    [GlobalSetup]
    public void SetupBenchmark() => StartNode();

    /// <summary>Stops the benchmark node.</summary>
    [GlobalCleanup]
    public void TeardownBenchmark() => StopNode();
}
