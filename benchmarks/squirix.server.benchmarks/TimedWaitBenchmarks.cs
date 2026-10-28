using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of one timed wait: a wait that a notification ends before the delay, and a wait on a task that already completed.</summary>
[MemoryDiagnoser]
public class TimedWaitBenchmarks
{
    private const int OperationsPerInvoke = 10_000;
    private static readonly TimeSpan Delay = TimeSpan.FromMinutes(1);
    private readonly ReplicaApplySignal _signal = new();
    private readonly CancellationTokenSource _cancellation = new();

    /// <summary>Gets or sets a value indicating whether the caller token can be canceled.</summary>
    [Params(false, true)]
    public bool CancelableToken { get; set; }

    /// <summary>Disposes the cancellation source.</summary>
    [GlobalCleanup]
    public void Cleanup() => _cancellation.Dispose();

    /// <summary>Waits for a task that already completed.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task CompletedTaskAsync()
    {
        var token = Token();
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await TimedWait.WaitAsync(Task.CompletedTask, Delay, TimeProvider.System, token).ConfigureAwait(false);
    }

    /// <summary>Starts a wait on the apply signal, then ends it with a notification.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task NotifiedWaitAsync()
    {
        var token = Token();
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var wait = _signal.WaitAsync(Delay, TimeProvider.System, token);
            _signal.Notify();
            _ = await wait.ConfigureAwait(false);
        }
    }

    private CancellationToken Token() => CancelableToken ? _cancellation.Token : CancellationToken.None;
}
