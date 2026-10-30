using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers operation budgets bounded by the timer of their current phase.</summary>
[Immutable]
public sealed class PhaseBudgetTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    /// <summary>One timer bounds an attempt on a shared clock, including an otherwise infinite attempt.</summary>
    /// <param name="scenario">Zero for attempt expiry, one for a shorter budget, two for an infinite attempt, or three for an early budget timer callback.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task SharedClockUsesOneAttemptTimer(int scenario)
    {
        var clock = new CountingClock();
        var timeout = scenario == 2 ? Timeout.InfiniteTimeSpan : TimeSpan.FromHours(1);
        var duration = scenario is 1 or 3 ? TimeSpan.FromMinutes(30) : TimeSpan.FromHours(2);
        await using var policy = new CallPolicy(clock, timeout, 1);
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime + duration, clock);
        using var entered = new SemaphoreSlim(0);
        var call = policy.ExecuteAsync(static (signal, ct) => WaitUntilCanceledAsync(signal, ct), entered, CancellationToken.None, budget).AsTask();
        _ = await Assert.That(await entered.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        var timerCount = clock.TimerCount;

        clock.Advance(scenario == 0 ? timeout : duration, scenario != 3);
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

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
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(1), clock);
        var state = new FailureState(httpFailure);
        var call = policy.ExecuteAsync(static (counter, _) => counter.FailAsync(), state, CancellationToken.None, budget).AsTask();
        clock.Advance(TimeSpan.FromMinutes(1) - TimeSpan.FromTicks(1), advanceTimestamp);
        var pending = !call.IsCompleted;
        clock.Advance(TimeSpan.FromTicks(1), advanceTimestamp);
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(state.Attempts).IsEqualTo(1);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    /// <summary>An ambiguous mutation outcome retains its no-retry contract when the budget timer has already fired.</summary>
    /// <param name="independentClocks">Whether the attempt and operation budget use different clock instances.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AmbiguousOutcomeWinsOverBudgetTimer(bool independentClocks)
    {
        var clock = new CountingClock();
        var budgetClock = independentClocks ? new CountingClock() : clock;
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(2), 3, TimeSpan.Zero, TimeSpan.Zero);
        var budget = DeadlineBudget.Start(budgetClock.GetUtcNow().UtcDateTime.AddHours(1), budgetClock);
        var response = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        int[] attempts = [0];
        var call = policy.ExecuteAsync(
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
        var failure = new RpcException(new Status(StatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));
        _ = response.TrySetException(failure);
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

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
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(1), clock);
        var state = new FailureState(false);
        var call = policy.ExecuteAsync(static (counter, _) => counter.FailAsync(), state, caller.Token, budget).AsTask();

        await caller.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));
        _ = await Assert.That(state.Attempts).IsEqualTo(1);
    }

    /// <summary>Time spent queued is not granted again when the attempt starts.</summary>
    [Test]
    public async Task QueuedAttemptKeepsSpentBudget()
    {
        var clock = new CountingClock();
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(1), 1, maxConcurrentPerPeer: 1);
        var release = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var holder = HoldSlotAsync(policy, release);
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(90), clock);
        using var entered = new SemaphoreSlim(0);
        var queued = policy.ExecuteAsync(static (signal, ct) => WaitUntilCanceledAsync(signal, ct), entered, CancellationToken.None, budget).AsTask();
        clock.Advance(TimeSpan.FromMinutes(30));
        _ = release.TrySetResult(1);
        _ = await holder.WaitAsync(Bound, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(await entered.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        _ = await Assert.That(clock.TimerCount).IsEqualTo(2);
        clock.Advance(TimeSpan.FromMinutes(60));
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(queued.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");

        static Task<int> HoldSlotAsync(CallPolicy policy, TaskCompletionSource<int> release)
        {
            return policy.ExecuteAsync(static (gate, _) => new ValueTask<int>(gate.Task), release, CancellationToken.None).AsTask();
        }
    }

    /// <summary>Retrying a timed-out attempt retains the time spent before the next attempt.</summary>
    [Test]
    public async Task RetryKeepsRemainingBudget()
    {
        var clock = new CountingClock();
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(1), 3, TimeSpan.Zero, TimeSpan.Zero);
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddMinutes(90), clock);
        using var attempts = new SemaphoreSlim(0);
        var call = policy.ExecuteAsync(static (signal, ct) => WaitUntilCanceledAsync(signal, ct), attempts, CancellationToken.None, budget).AsTask();
        _ = await Assert.That(await attempts.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        clock.Advance(TimeSpan.FromHours(1));
        _ = await Assert.That(await attempts.WaitAsync(Bound, CancellationToken.None)).IsTrue();
        var timerCount = clock.TimerCount;
        clock.Advance(TimeSpan.FromMinutes(30));
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(timerCount).IsEqualTo(2);
        _ = await Assert.That(attempts.CurrentCount).IsEqualTo(0);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    /// <summary>An operation whose budget is already spent never enters the action or creates a timer.</summary>
    [Test]
    public async Task SpentBudgetDoesNotStartAttempt()
    {
        var clock = new CountingClock();
        await using var policy = new CallPolicy(clock);
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddSeconds(-1), clock);
        var state = new FailureState(false);
        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(static (counter, _) => counter.FailAsync(), state, CancellationToken.None, budget));

        _ = await Assert.That(state.Attempts).IsEqualTo(0);
        _ = await Assert.That(clock.TimerCount).IsEqualTo(0);
        _ = await Assert.That(error.Status.Detail).IsEqualTo("Request deadline exceeded.");
    }

    private static async ValueTask<int> WaitUntilCanceledAsync(SemaphoreSlim entered, CancellationToken cancellationToken)
    {
        _ = entered.Release();
        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        return 1;
    }

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
