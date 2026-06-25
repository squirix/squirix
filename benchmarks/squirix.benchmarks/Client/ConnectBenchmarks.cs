using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Support.Cluster;

namespace Squirix.Benchmarks.Client;

/// <summary>Phase-1 remote client benchmark: connect and dispose per iteration.</summary>
[MemoryDiagnoser]
public sealed class ClientConnectBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>Measures client bootstrap and teardown against a node started in global setup.</summary>
    /// <returns>A task that completes after the client is disposed.</returns>
    [Benchmark]
    [InvocationCount(1)]
    public Task ConnectAndDisposeAsync() => ConnectAndDisposeClientAsync();

    /// <summary>Starts the benchmark node.</summary>
    /// <returns>A task that completes after the node is started.</returns>
    [GlobalSetup]
    public Task SetupBenchmarkAsync() => StartNodeAsync();

    /// <summary>Stops the benchmark node.</summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    [GlobalCleanup]
    public Task TeardownBenchmarkAsync() => StopNodeAsync();
}
