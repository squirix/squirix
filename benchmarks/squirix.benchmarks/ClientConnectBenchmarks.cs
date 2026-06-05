using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Phase-1 remote client benchmark: connect and dispose per iteration.
/// </summary>
[MemoryDiagnoser]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class ClientConnectBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>
    /// Measures client bootstrap and teardown against a node started in global setup.
    /// </summary>
    [Benchmark]
    [InvocationCount(1)]
    public void ConnectAndDispose() => ConnectAndDisposeClient();

    /// <summary>Starts the benchmark node.</summary>
    [GlobalSetup]
    public void SetupBenchmark() => StartNode();

    /// <summary>Stops the benchmark node.</summary>
    [GlobalCleanup]
    public void TeardownBenchmark() => StopNode();
}
