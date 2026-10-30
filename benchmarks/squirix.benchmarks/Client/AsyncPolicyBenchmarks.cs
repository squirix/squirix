using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;

namespace Squirix.Benchmarks.Client;

/// <summary>Measures reliability wrappers when the operation suspends, as a remote call does.</summary>
[MemoryDiagnoser]
[InProcess]
[WarmupCount(2)]
[IterationCount(5)]
public class AsyncPolicyBenchmarks
{
    private CallPolicy? _policy;
    private EndpointFailover? _failover;

    /// <summary>Gets or sets a value indicating whether bootstrap failover supplies the overall deadline.</summary>
    [Params(false, true)]
    public bool WithDeadline { get; set; }

    /// <summary>Creates reusable client wrappers.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _policy = new CallPolicy(TimeProvider.System, peer: "async-benchmark");
        _failover = new EndpointFailover(["node-a"], "node-a", TimeSpan.FromSeconds(30), TimeProvider.System);
    }

    /// <summary>Executes one asynchronously completing request.</summary>
    /// <returns>The request result.</returns>
    [Benchmark]
    public ValueTask<int> ExecuteAsync() => WithDeadline
        ? _failover!.ExecuteAsync(static (_, policy, ct) => policy.ExecuteAsync(static (_, token) => SuspendAsync(token), 0, ct), _policy!, CancellationToken.None)
        : _policy!.ExecuteAsync(static (_, token) => SuspendAsync(token), 0, CancellationToken.None);

    /// <summary>Drains and disposes the policy.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_policy != null)
            await _policy.DisposeAsync().ConfigureAwait(false);
    }

    private static async ValueTask<int> SuspendAsync(CancellationToken cancellationToken)
    {
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        return 42;
    }
}
