using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Internal;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;

namespace Squirix.Benchmarks.Client;

/// <summary>Isolates client-side reliability and bootstrap wrappers without gRPC transport.</summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class PolicyOverheadBenchmarks : IAsyncDisposable
{
    private const int Batch = 16_384;
    private static readonly string[] SingleBootstrapNode = ["node-a"];
    private readonly Consumer _consumer = new();
    private EndpointFailover? _failover;
    private CallPolicy? _policy;

    /// <summary>Records the queue-wait metric alone, isolating metric tag overhead from timeout and semaphore costs.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public static void QueueWaitMetricObserveBatched()
    {
        for (var i = 0; i < Batch; i++)
            CallPolicyMetrics.ObserveQueueWaitSeconds("node-a", TimeSpan.Zero);
    }

    /// <summary>Runs through bootstrap failover and call policy, matching the public SDK wrapper shape without gRPC.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task BootstrapCallPolicyDoneVtBatchedAsync()
    {
        var failover = _failover!;
        var policy = _policy!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await failover.ExecuteAsync(
                static (_, policyState, ct) => policyState.ExecuteAsync(static (_, token) => CompletedValueTaskAsync(token), 0, ct),
                policy,
                CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result);
        }
    }

    /// <summary>
    /// Runs through <see cref="EndpointFailover" /> only.
    /// </summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task BootstrapFailoverCompletedValueTaskBatchedAsync()
    {
        var failover = _failover!;
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(await failover.ExecuteAsync(static (_, ct) => CompletedValueTaskAsync(ct), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Runs through <see cref="CallPolicy" /> only.
    /// </summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task CallPolicyCompletedValueTaskBatchedAsync()
    {
        var policy = _policy!;
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(await policy.ExecuteAsync(static ct => CompletedValueTaskAsync(ct), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Releases benchmark resources.</summary>
    [GlobalCleanup]
    public ValueTask CleanupAsync() => DisposeAsync();

    /// <summary>
    /// Runs a baseline completed <see cref="ValueTask{TResult}" /> without wrappers.
    /// </summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = Batch)]
    public void DirectCompletedValueTaskBatched()
    {
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(42);
    }

    /// <summary>Creates reusable wrapper instances.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _failover = new EndpointFailover(["node-a"], "node-a");
        _policy = new CallPolicy(peer: "node-a");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_policy is not null)
        {
            await _policy.DisposeAsync().ConfigureAwait(false);
            _policy = null;
        }

        GC.SuppressFinalize(this);
    }

    private static ValueTask<int> CompletedValueTaskAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<int>(42);
    }
}
