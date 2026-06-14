using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;

namespace Squirix.Benchmarks;

/// <summary>
/// Phase-1 remote client benchmark: connect and dispose per iteration.
/// </summary>
[MemoryDiagnoser]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class ClientConnectBenchmarks : RemoteBenchmarkLifecycleBase
{
    /// <summary>
    /// Measures client bootstrap and teardown against a node started in global setup.
    /// </summary>
    /// <returns>A task that completes after the client is disposed.</returns>
    [Benchmark]
    [InvocationCount(1)]
    public async Task ConnectAndDisposeAsync() => await ConnectAndDisposeClientAsync().ConfigureAwait(false);

    /// <summary>Starts the benchmark node.</summary>
    /// <returns>A task that completes after the node is started.</returns>
    [GlobalSetup]
    public async Task SetupBenchmarkAsync() => await StartNodeAsync().ConfigureAwait(false);

    /// <summary>Stops the benchmark node.</summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    [GlobalCleanup]
    public async Task TeardownBenchmarkAsync() => await StopNodeAsync().ConfigureAwait(false);
}
