using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Internal.Cluster.Bootstrap;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;

namespace Squirix.Benchmarks;

/// <summary>
/// Isolates client-side reliability and bootstrap wrappers without gRPC transport.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class ClientPolicyOverheadBenchmarks : IAsyncDisposable
{
    private const int Batch = 16_384;
    private readonly Consumer _consumer = new();
    private BootstrapEndpointFailover? _failover;
    private CallPolicy? _policy;

    /// <summary>
    /// Runs a baseline completed <see cref="ValueTask{TResult}" /> without wrappers.
    /// </summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = Batch)]
    public void DirectCompletedValueTaskBatched()
    {
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(42);
    }

    /// <summary>
    /// Runs through <see cref="BootstrapEndpointFailover" /> only.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is done.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task BootstrapFailoverCompletedValueTaskBatched()
    {
        var failover = _failover!;
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(await failover.ExecuteAsync(static (_, ct) => CompletedValueTask(ct), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Runs through <see cref="CallPolicy" /> only.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is done.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task CallPolicyCompletedValueTaskBatched()
    {
        var policy = _policy!;
        for (var i = 0; i < Batch; i++)
            _consumer.Consume(await policy.ExecuteAsync(static ct => CompletedValueTask(ct), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Records the queue-wait metric alone, isolating metric tag overhead from timeout and semaphore costs.
    /// </summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void QueueWaitMetricObserveBatched()
    {
        for (var i = 0; i < Batch; i++)
            CallPolicyMetrics.QueueWaitSeconds.Observe("node-a", TimeSpan.Zero);
    }

    /// <summary>
    /// Runs through bootstrap failover and call policy, matching the public SDK wrapper shape without gRPC.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is done.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task BootstrapAndCallPolicyCompletedValueTaskBatched()
    {
        var failover = _failover!;
        var policy = _policy!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await failover.ExecuteAsync(
                (_, ct) => policy.ExecuteAsync(static token => CompletedValueTask(token), ct),
                CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result);
        }
    }

    /// <summary>
    /// Runs through bootstrap failover and call policy using state overloads, matching the optimized wrapper shape.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is done.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public async Task BootstrapAndCallPolicyStateOverloadCompletedValueTaskBatched()
    {
        var failover = _failover!;
        var policy = _policy!;
        for (var i = 0; i < Batch; i++)
        {
            var result = await failover.ExecuteAsync(
                static (_, policyState, ct) => policyState.ExecuteAsync(static (_, token) => CompletedValueTask(token), 0, ct),
                policy,
                CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(result);
        }
    }

    /// <summary>
    /// Releases benchmark resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that completes when cleanup is done.</returns>
    [GlobalCleanup]
    public ValueTask CleanupAsync() => DisposeAsync();

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

    /// <summary>
    /// Creates reusable wrapper instances.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _failover = new BootstrapEndpointFailover(["node-a"], "node-a");
        _policy = new CallPolicy(peer: "node-a");
    }

    private static ValueTask<int> CompletedValueTask(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<int>(42);
    }
}
