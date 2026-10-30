using System;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Covers operation budgets bounded by the timer of their current phase.</summary>
[Immutable]
public sealed class ServerPhaseBudgetTests : DisposableServerUnitTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);
    private readonly Meter _testMeter = new("test-server-phase-budget");

    /// <summary>One timer bounds an attempt on a shared clock, including an otherwise infinite attempt.</summary>
    /// <param name="scenario">Zero for attempt expiry, one for a shorter budget, two for an infinite attempt, or three for an early budget timer callback.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task SharedClockUsesOneAttemptTimer(int scenario)
    {
        using var hangGuard = new CancellationTokenSource(Bound + Bound);
        var clock = new CountingClock();
        var timeout = scenario == 2 ? Timeout.InfiniteTimeSpan : TimeSpan.FromHours(1);
        var duration = scenario is 1 or 3 ? TimeSpan.FromMinutes(30) : TimeSpan.FromHours(2);
        await using var policy = CreatePolicy(clock, timeout, 1);
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime + duration, clock);
        using var entered = new SemaphoreSlim(0);
        var call = ExecuteAsync(policy, static (signal, ct) => WaitUntilCanceledAsync(signal, ct), entered, hangGuard.Token, budget).AsTask();
        _ = await Assert.That(await entered.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        var timerCount = clock.TimerCount;

        clock.Advance(scenario == 0 ? timeout : duration, scenario != 3);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(timerCount).IsEqualTo(1);
        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(error.Status.Detail).IsEqualTo(scenario == 0 ? "All attempts timed out." : "Request deadline exceeded.");
    }

    /// <summary>A backoff cannot extend the shared operation budget or start another attempt after expiry.</summary>
    /// <param name="httpFailure">Whether the attempt fails with an HTTP transport error instead of a gRPC error.</param>
    /// <param name="advanceTimestamp">Whether the clock timestamp advances along with its timer callbacks.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task BudgetBoundsBackoff(bool httpFailure, bool advanceTimestamp)
    {
        var clock = new CountingClock();
        await using var policy = CreatePolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(1), clock);
        var state = new FailureState(httpFailure);
        var call = ExecuteAsync(policy, static (counter, _) => counter.FailAsync(), state, CancellationToken.None, budget).AsTask();
        clock.Advance(TimeSpan.FromMinutes(1) - TimeSpan.FromTicks(1), advanceTimestamp);
        var pending = !call.IsCompleted;
        clock.Advance(TimeSpan.FromTicks(1), advanceTimestamp);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(state.Attempts).IsEqualTo(1);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    /// <summary>A terminal mutation or topology failure retains its no-retry contract when the budget timer has already fired.</summary>
    /// <param name="independentClocks">Whether the attempt and operation budget use different clock instances.</param>
    /// <param name="terminal">Zero for an ambiguous outcome, one for a ring mismatch, or two for a fenced ring.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task TerminalFailureWinsOverBudgetTimer(bool independentClocks, int terminal)
    {
        var clock = new CountingClock();
        var budgetClock = independentClocks ? new CountingClock() : clock;
        await using var policy = CreatePolicy(clock, TimeSpan.FromHours(2), 3, TimeSpan.Zero, TimeSpan.Zero);
        var budget = ServerDeadlineBudget.Start(budgetClock.GetUtcNow().UtcDateTime.AddHours(1), budgetClock);
        var response = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        int[] attempts = [0];
        var call = ExecuteAsync(
            policy,
            static (state, ct) =>
            {
                _ = ct;
                _ = Interlocked.Increment(ref state.Attempts[0]);
                return new ValueTask<int>(state.Response.Task);
            },
            (Attempts: attempts, Response: response),
            CancellationToken.None,
            budget).AsTask();
        budgetClock.Advance(TimeSpan.FromHours(1), false);
        var failure = terminal switch
        {
            0 => new RpcException(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)),
            1 => RingMismatchFailure.Mismatch(),
            _ => RingMismatchFailure.Fenced(),
        };
        _ = response.TrySetException(failure);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(error).IsSameReferenceAs(failure);
        _ = await Assert.That(attempts[0]).IsEqualTo(1);
        _ = await Assert.That(budget.Remaining).IsEqualTo(TimeSpan.FromHours(1));
    }

    /// <summary>Caller cancellation during a budget-limited backoff remains caller cancellation.</summary>
    [Test]
    public async Task CallerCancelDuringBackoff()
    {
        var clock = new CountingClock();
        using var caller = new CancellationTokenSource();
        await using var policy = CreatePolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(1), clock);
        var state = new FailureState(false);
        var call = ExecuteAsync(policy, static (counter, _) => counter.FailAsync(), state, caller.Token, budget).AsTask();

        await caller.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));
        _ = await Assert.That(state.Attempts).IsEqualTo(1);
    }

    /// <summary>Retrying a timed-out attempt retains the time spent before the next attempt.</summary>
    [Test]
    public async Task RetryKeepsRemainingBudget()
    {
        var clock = new CountingClock();
        await using var policy = CreatePolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.Zero, TimeSpan.Zero);
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(90), clock);
        using var attempts = new SemaphoreSlim(0);
        var call = ExecuteAsync(policy, static (signal, ct) => WaitUntilCanceledAsync(signal, ct), attempts, CancellationToken.None, budget).AsTask();
        _ = await Assert.That(await attempts.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        clock.Advance(TimeSpan.FromHours(1));
        _ = await Assert.That(await attempts.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        var timerCount = clock.TimerCount;
        clock.Advance(TimeSpan.FromMinutes(30));
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(timerCount).IsEqualTo(2);
        _ = await Assert.That(attempts.CurrentCount).IsEqualTo(0);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    /// <summary>An operation whose budget is already spent never enters the action or creates a timer.</summary>
    [Test]
    public async Task SpentBudgetDoesNotStartAttempt()
    {
        var clock = new CountingClock();
        await using var policy = CreatePolicy(clock);
        var budget = ServerDeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddSeconds(-1), clock);
        var state = new FailureState(false);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(ExecuteAsync(policy, static (counter, _) => counter.FailAsync(), state, CancellationToken.None, budget));

        _ = await Assert.That(state.Attempts).IsEqualTo(0);
        _ = await Assert.That(clock.TimerCount).IsEqualTo(0);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    /// <summary>Independent clocks retain separate operation and attempt timers.</summary>
    [Test]
    public async Task IndependentClocksRetainBothLimits()
    {
        var clock = new CountingClock();
        var budgetClock = new CountingClock();
        await using var policy = CreatePolicy(clock, TimeSpan.FromHours(1), 1);
        var budget = ServerDeadlineBudget.Start(budgetClock.GetUtcNow().UtcDateTime.AddHours(2), budgetClock);
        using var entered = new SemaphoreSlim(0);
        var call = ExecuteAsync(policy, static (signal, ct) => WaitUntilCanceledAsync(signal, ct), entered, CancellationToken.None, budget).AsTask();
        _ = await Assert.That(await entered.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        _ = await Assert.That(clock.TimerCount).IsEqualTo(1);
        _ = await Assert.That(budgetClock.TimerCount).IsEqualTo(1);
        clock.Advance(TimeSpan.FromHours(1));
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(error.Status.Detail).IsEqualTo("All attempts timed out.");
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async ValueTask<T> ExecuteAsync<TState, T>(ServerCallPolicy policy, Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken, ServerDeadlineBudget? budget = null)
    {
        using var deadline = budget is { } value ? ServerRpcDeadlineContext.Push(in value) : ServerRpcDeadlineContext.Push(null, TimeProvider.System);
        return await policy.ExecuteAsync(state, action, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<int> WaitUntilCanceledAsync(SemaphoreSlim entered, CancellationToken cancellationToken)
    {
        _ = entered.Release();
        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        return 1;
    }

    private ServerCallPolicy CreatePolicy(TimeProvider clock, TimeSpan? timeout = null, int attempts = 3, TimeSpan? baseBackoff = null, TimeSpan? maxBackoff = null) => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        clock,
        attempts,
        timeouts: new CallPolicyTimeouts(timeout, baseBackoff, maxBackoff));

    private sealed class CountingClock : TimeProvider
    {
        private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        private long _timestamp;
        private int _timerCount;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        internal int TimerCount => Volatile.Read(ref _timerCount);

        public override DateTimeOffset GetUtcNow() => _clock.GetUtcNow();

        public override long GetTimestamp() => Volatile.Read(ref _timestamp);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = Interlocked.Increment(ref _timerCount);
            return _clock.CreateTimer(callback, state, dueTime, period);
        }

        internal void Advance(TimeSpan elapsed, bool advanceTimestamp = true)
        {
            if (advanceTimestamp)
                _ = Interlocked.Add(ref _timestamp, elapsed.Ticks);
            _clock.Advance(elapsed);
        }
    }

    private sealed class FailureState
    {
        private readonly bool _httpFailure;
        private int _attempts;

        internal FailureState(bool httpFailure)
        {
            _httpFailure = httpFailure;
        }

        internal int Attempts => Volatile.Read(ref _attempts);

        internal ValueTask<int> FailAsync()
        {
            _ = Interlocked.Increment(ref _attempts);
            return ValueTask.FromException<int>(_httpFailure ? new HttpRequestException("down") : new RpcException(new Status(StatusCode.Unavailable, "down")));
        }
    }
}
