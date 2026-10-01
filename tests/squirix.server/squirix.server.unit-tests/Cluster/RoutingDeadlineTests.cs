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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>One absolute deadline is shared by the server reroute and its transport retries.</summary>
[Immutable]
public sealed class RoutingDeadlineTests : DisposableServerUnitTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private readonly Meter _testMeter = new("test-routing-deadline");

    /// <summary>The reroute budget and the transport retry loop observe the same absolute deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// The mandated name is "RerouteAndTransportRetriesShareAbsoluteDeadline"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Test]
    public async Task RerouteAndRetriesShareDeadline(CancellationToken cancellationToken)
    {
        var deadlineUtc = DateTime.UtcNow.AddSeconds(30);
        var budget = new RerouteBudget(new DateTimeOffset(deadlineUtc, TimeSpan.Zero), TimeProvider.System);
        using var scope = ServerRpcDeadlineContext.Push(deadlineUtc, TimeProvider.System);

        var transportRemaining = ServerRpcDeadlineContext.GetRemainingBudget();
        _ = await Assert.That(transportRemaining).IsNotNull();
        var rerouteRemaining = budget.GetRemaining();
        _ = await Assert.That(rerouteRemaining > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(rerouteRemaining <= TimeSpan.FromSeconds(30)).IsTrue();
        _ = await Assert.That(budget.HasExpired()).IsFalse();

        // The shared deadline advances under a fake clock without touching the ambient budget above.
        var time = new FakeTimeProvider(new DateTimeOffset(deadlineUtc, TimeSpan.Zero) - TimeSpan.FromSeconds(30));
        var paced = new RerouteBudget(time.GetUtcNow() + TimeSpan.FromSeconds(10), time);
        _ = await Assert.That(paced.HasExpired()).IsFalse();
        time.Advance(TimeSpan.FromSeconds(11));
        _ = await Assert.That(paced.HasExpired()).IsTrue();

        // An expired shared deadline rejects transport attempts before the first try.
        await using var policy = CreatePolicy(peer: "reroute-deadline", timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();
        using var expired = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(-1), TimeProvider.System);
        var expiredBudget = new RerouteBudget(DateTimeOffset.UtcNow.AddSeconds(-1), TimeProvider.System);
        _ = await Assert.That(expiredBudget.HasExpired()).IsTrue();

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

    /// <summary>The server call policy drives retry backoff from the injected time provider.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServerCallPolicyUsesInjectedTimeProvider(CancellationToken cancellationToken)
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
            cancellationToken);

        while (attempts.Count < 1)
            await Task.Yield();

        // The retry backoff is parked on the fake clock: nothing runs until time advances.
        _ = await Assert.That(executeTask.IsCompleted).IsFalse();
        timeProvider.Advance(TimeSpan.FromMinutes(1));
        _ = await Assert.That(await executeTask).IsEqualTo(9);
        _ = await Assert.That(attempts.Count).IsEqualTo(2);
    }

    /// <summary>A pushed budget counts down on monotonic time: a wall-clock step after the push leaves it unchanged.</summary>
    [Test]
    public async Task WallStepLeavesPushedBudget()
    {
        var clock = new SteppedWallClock();
        using var scope = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + TimeSpan.FromHours(1), clock);

        clock.StepWallClock(TimeSpan.FromHours(2));
        var afterStep = ServerRpcDeadlineContext.GetRemainingBudget();
        clock.Advance(TimeSpan.FromMinutes(10));

        _ = await Assert.That(afterStep).IsEqualTo(TimeSpan.FromHours(1));
        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromMinutes(50));
    }

    /// <summary>The operation budget expires on the clock its deadline was pushed with, with no real delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetExpiresOnPushClock(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        await using var policy = CreatePolicy(new CallPolicyTimeouts(TimeSpan.FromHours(2), TimeSpan.Zero, TimeSpan.Zero), 1, peer: "budget-clock", timeProvider: clock);
        using var scope = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + TimeSpan.FromHours(1), clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var call = policy.ExecuteAsync(entered, static (signal, ct) => BlockUntilCanceledAsync(signal, ct), cancellationToken).AsTask();
        await entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        clock.StepWallClock(TimeSpan.FromHours(2));
        var pendingAfterStep = !call.IsCompleted;
        clock.Advance(TimeSpan.FromHours(1));
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, cancellationToken)));

        _ = await Assert.That(pendingAfterStep).IsTrue();
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>The budget counts down on the clock its deadline was pushed with, even when the policy runs on another clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetFollowsPushClock(CancellationToken cancellationToken)
    {
        var pushClock = new SteppedWallClock();
        await using var policy = CreatePolicy(new CallPolicyTimeouts(TimeSpan.FromHours(2), TimeSpan.Zero, TimeSpan.Zero), 1, peer: "budget-push-clock", timeProvider: TimeProvider.System);
        using var scope = ServerRpcDeadlineContext.Push(pushClock.GetUtcNow().UtcDateTime + TimeSpan.FromHours(1), pushClock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var call = policy.ExecuteAsync(entered, static (signal, ct) => BlockUntilCanceledAsync(signal, ct), cancellationToken).AsTask();
        await entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        pushClock.Advance(TimeSpan.FromHours(1));
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, cancellationToken)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>The per-attempt timeout runs on the policy clock, with no real delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AttemptTimeoutRunsOnPolicyClock(CancellationToken cancellationToken)
    {
        var clock = new SteppedWallClock();
        await using var policy = CreatePolicy(new CallPolicyTimeouts(TimeSpan.FromHours(1), TimeSpan.Zero, TimeSpan.Zero), 1, peer: "attempt-clock", timeProvider: clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var call = policy.ExecuteAsync(entered, static (signal, ct) => BlockUntilCanceledAsync(signal, ct), cancellationToken).AsTask();
        await entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var pending = !call.IsCompleted;
        clock.Advance(TimeSpan.FromHours(1));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, cancellationToken)));

        _ = await Assert.That(pending).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async ValueTask<int> BlockUntilCanceledAsync(TaskCompletionSource entered, CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        return 0;
    }

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

    /// <summary>A fake clock whose wall time can step while its monotonic timestamp moves forward only.</summary>
    [ThreadSafe]
    private sealed class SteppedWallClock : FakeTimeProvider
    {
        private long _wallOffsetTicks;

        internal SteppedWallClock()
            : base(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
        {
        }

        public override DateTimeOffset GetUtcNow() => base.GetUtcNow().AddTicks(Interlocked.Read(ref _wallOffsetTicks));

        /// <summary>Reads the unstepped time, since the base derives timestamps from the virtual wall time; the monotonic clock never moves back.</summary>
        /// <returns>The monotonic timestamp.</returns>
        public override long GetTimestamp() => base.GetUtcNow().UtcTicks;

        internal void StepWallClock(TimeSpan step) => _ = Interlocked.Add(ref _wallOffsetTicks, step.Ticks);
    }

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
