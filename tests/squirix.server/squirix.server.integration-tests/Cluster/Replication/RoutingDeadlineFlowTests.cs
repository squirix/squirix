using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Routing deadline flow: one absolute deadline bounds the reroute and the transport retries.</summary>
public sealed class RoutingDeadlineFlowTests : NodeIntegrationTestBase
{
    /// <summary>An expired shared deadline rejects transport attempts before the first try.</summary>
    [Fact]
    public async Task ExpiredDeadlineRejectsTransportAttempts()
    {
        using var meter = new Meter("test-routing-flow");
        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter), new ServerRpcTimeoutMetrics(meter));
        await using var policy = new ServerCallPolicy(instrumentation, 3, 64, "routing-flow", TimeProvider.System, new CallPolicyTimeouts());

        var attempts = new InvocationCounter();
        using var expired = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(-1));
        var budget = new RerouteBudget(DateTimeOffset.UtcNow.AddSeconds(-1), TimeProvider.System);
        Assert.True(budget.HasExpired());

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                attempts,
                static (counter, _) =>
                {
                    var attempt = counter.Increment();
                    return ValueTask.FromResult(attempt);
                },
                DefaultCancellationToken));
        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);
        Assert.Equal(0, attempts.Count);
    }

    /// <summary>A stale term consumes the single reroute while the shared deadline still bounds retries.</summary>
    [Fact]
    public void StaleTermRerouteSharesDeadline()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var budget = new RerouteBudget(time.GetUtcNow() + TimeSpan.FromSeconds(10), time);

        Assert.Equal(StaleTermVerdict.Stale, StaleTermClassifier.Classify(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm))));
        Assert.True(budget.TryConsumeReroute());
        Assert.False(budget.HasExpired());

        time.Advance(TimeSpan.FromSeconds(11));
        Assert.True(budget.HasExpired());
        Assert.False(budget.TryConsumeReroute());
    }

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
