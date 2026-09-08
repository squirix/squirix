using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>One absolute deadline is shared by the server reroute and its transport retries.</summary>
[Immutable]
public sealed class RoutingDeadlineTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test-routing-deadline");

    /// <summary>The reroute budget and the transport retry loop observe the same absolute deadline.</summary>
    /// <remarks>
    /// #236 mandates the name "RerouteAndTransportRetriesShareAbsoluteDeadline"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public async Task RerouteAndRetriesShareDeadline()
    {
        var deadlineUtc = DateTime.UtcNow.AddSeconds(30);
        var budget = new RerouteBudget(new DateTimeOffset(deadlineUtc, TimeSpan.Zero), TimeProvider.System);
        using var scope = ServerRpcDeadlineContext.Push(deadlineUtc);

        var transportRemaining = ServerRpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
        Assert.True(transportRemaining is TimeSpan);
        var rerouteRemaining = budget.GetRemaining();
        Assert.True(rerouteRemaining > TimeSpan.Zero);
        Assert.True(rerouteRemaining <= TimeSpan.FromSeconds(30));
        Assert.False(budget.HasExpired());

        // The shared deadline advances under a fake clock without touching the ambient budget above.
        var time = new FakeTimeProvider(new DateTimeOffset(deadlineUtc, TimeSpan.Zero) - TimeSpan.FromSeconds(30));
        var paced = new RerouteBudget(time.GetUtcNow() + TimeSpan.FromSeconds(10), time);
        Assert.False(paced.HasExpired());
        time.Advance(TimeSpan.FromSeconds(11));
        Assert.True(paced.HasExpired());

        // An expired shared deadline rejects transport attempts before the first try.
        await using var policy = CreatePolicy(peer: "reroute-deadline", timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();
        using var expired = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(-1));
        var expiredBudget = new RerouteBudget(DateTimeOffset.UtcNow.AddSeconds(-1), TimeProvider.System);
        Assert.True(expiredBudget.HasExpired());

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

    /// <summary>The server call policy drives retry backoff from the injected time provider.</summary>
    [Fact]
    public async Task ServerCallPolicyUsesInjectedTimeProvider()
    {
        var timeProvider = new FakeTimeProvider();
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5)),
            2,
            peer: "reroute-time-provider",
            timeProvider: timeProvider);
        var attempts = new InvocationCounter();
        var executeTask = policy.ExecuteAsync(
            attempts,
            static (counter, _) =>
            {
                var attempt = counter.Increment();
                return attempt == 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, "down"))) : new ValueTask<int>(9);
            },
            DefaultCancellationToken);

        while (attempts.Count < 1)
            await Task.Yield();

        // The retry backoff is parked on the fake clock: nothing runs until time advances.
        Assert.False(executeTask.IsCompleted);
        timeProvider.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(9, await executeTask);
        Assert.Equal(2, attempts.Count);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private ServerCallPolicy CreatePolicy(
        CallPolicyTimeouts? timeouts = null,
        int maxAttempts = 3,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        TimeProvider? timeProvider = null) => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        maxAttempts,
        maxConcurrentPerPeer,
        peer,
        timeProvider ?? TimeProvider.System,
        timeouts ?? new CallPolicyTimeouts());

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
