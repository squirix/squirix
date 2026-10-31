using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures an asynchronous owner-forward policy with and without an operation budget.</summary>
[MemoryDiagnoser]
public class ServerCallPolicyBudgetBenchmarks
{
    private ServerDeadlineBudget _budget;
    private Meter? _meter;
    private ServerCallPolicy? _policy;

    /// <summary>Gets or sets a value indicating whether the call carries an ambient operation budget.</summary>
    [Params(false, true)]
    public bool WithBudget { get; set; }

    /// <summary>Releases the policy and its instrumentation.</summary>
    /// <returns>A task that completes when active calls have drained.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_policy != null)
            await _policy.DisposeAsync().ConfigureAwait(false);

        _meter?.Dispose();
    }

    /// <summary>Executes an action that suspends once under the owner-forward policy.</summary>
    /// <returns>The action result.</returns>
    [Benchmark]
    public async Task<int> ExecuteAsync()
    {
        var policy = ThrowHelper.Required(_policy, "Benchmark policy was not initialized.");
        using var deadline = WithBudget ? ServerRpcDeadlineContext.Push(in _budget) : null;
        return await policy.ExecuteAsync(
                               42,
                               static async (value, _) =>
                               {
                                   await Task.Yield();
                                   return value;
                               },
                               CancellationToken.None)
                           .ConfigureAwait(false);
    }

    /// <summary>Creates a policy whose attempt limit is shorter than the operation budget.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("server-policy-budget-benchmark");
        _policy = new ServerCallPolicy(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_meter), new ServerRpcTimeoutMetrics(_meter)), TimeProvider.System);
        _budget = ServerDeadlineBudget.Start(DateTime.UtcNow.AddHours(1), TimeProvider.System);
    }
}
