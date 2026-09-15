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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Routing deadline flow: one absolute deadline bounds the reroute and the transport retries.</summary>
public sealed class RoutingDeadlineFlowTests : NodeIntegrationTestBase
{
    /// <summary>An expired shared deadline rejects transport attempts before the first try.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredDeadlineRejectsTransportAttempts(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-routing-flow");
        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter), new ServerRpcTimeoutMetrics(meter));
        await using var policy = new ServerCallPolicy(instrumentation, 3, 64, "routing-flow", TimeProvider.System, new CallPolicyTimeouts());

        var attempts = new InvocationCounter();
        using var expired = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(-1));
        var budget = new RerouteBudget(DateTimeOffset.UtcNow.AddSeconds(-1), TimeProvider.System);
        _ = await Assert.That(budget.HasExpired()).IsTrue();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                attempts,
                static (counter, _) =>
                {
                    var attempt = counter.Increment();
                    return ValueTask.FromResult(attempt);
                },
                cancellationToken));
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(attempts.Count).IsEqualTo(0);
    }

    /// <summary>A stale term consumes the single reroute while the shared deadline still bounds retries.</summary>
    [Test]
    public async Task StaleTermRerouteSharesDeadline()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var budget = new RerouteBudget(time.GetUtcNow() + TimeSpan.FromSeconds(10), time);

        _ = await Assert.That(StaleTermClassifier.Classify(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm)))).IsEqualTo(StaleTermVerdict.Stale);
        _ = await Assert.That(budget.TryConsumeReroute()).IsTrue();
        _ = await Assert.That(budget.HasExpired()).IsFalse();

        time.Advance(TimeSpan.FromSeconds(11));
        _ = await Assert.That(budget.HasExpired()).IsTrue();
        _ = await Assert.That(budget.TryConsumeReroute()).IsFalse();
    }

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
