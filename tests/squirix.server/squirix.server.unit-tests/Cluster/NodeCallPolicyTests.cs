using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>
/// Unit tests for deadline-aware retry and timeout handling in <see cref="ServerCallPolicy" />.
/// </summary>
[Immutable]
public sealed class NodeCallPolicyTests : ServerUnitTestBase
{
    /// <summary>Ensures the ambient request deadline caps the overall retry budget.</summary>
    [Fact]
    public async Task AmbientDeadlineCapsOverallRetryBudget()
    {
        await using var policy = CreatePolicy(
            TimeSpan.FromSeconds(5),
            5,
            TimeSpan.FromMilliseconds(5),
            TimeSpan.FromMilliseconds(5),
            peer: "peer-a",
            timeProvider: TimeProvider.System);
        using var deadline = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(50));

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                0,
                static async (_, token) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                    return 1;
                },
                DefaultCancellationToken));

        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);
    }

    /// <summary>Ensures draining a policy rejects new peer RPC execution immediately.</summary>
    [Fact]
    public async Task BeginDrainRejectsNewCalls()
    {
        using var sink = new NodeMeasurementSink("Squirix");
        await using var policy = CreatePolicy(peer: "peer-c", timeProvider: TimeProvider.System);
        policy.BeginDrain();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-c"), ("scope", "policy")));
    }

    /// <summary>Ensures the per-peer concurrency cap does not allow more concurrent executions than configured.</summary>
    [Fact]
    public async Task ConcurrencyCapSerializesExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        await using var policy = CreatePolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-e", timeProvider: TimeProvider.System);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var peakRunning = new PeakCounter();
        var sync = new ConcurrencySyncState(firstEntered, releaseFirst, peakRunning);

        var first = policy.ExecuteAsync(
            sync,
            static async (s, ct) =>
            {
                s.Peak.Record(s.Running.Increment());
                try
                {
                    s.FirstEntered.SetResult();
                    await s.ReleaseFirst.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                }
                finally
                {
                    _ = s.Running.Decrement();
                }

                return 1;
            },
            DefaultCancellationToken);
        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

        var second = policy.ExecuteAsync(
            sync,
            static (s, __) =>
            {
                s.Peak.Record(s.Running.Increment());
                try
                {
                    return ValueTask.FromResult(2);
                }
                finally
                {
                    _ = s.Running.Decrement();
                }
            },
            DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(30), TimeProvider.System, DefaultCancellationToken);
        Assert.False(second.IsCompleted);

        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        Assert.Equal(2, await second);
        Assert.Equal(1, peakRunning.Peak);
    }

    /// <summary>Ensures outbound call options inherit the ambient deadline budget.</summary>
    [Fact]
    public void DeadlineContextComputesCallDeadline()
    {
        using var scope = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(2));

        var effective = ServerRpcDeadlineContext.EffectiveDeadline(DateTime.UtcNow.AddSeconds(5));

        _ = Assert.NotNull(effective);
        Assert.True(effective <= DateTime.UtcNow.AddSeconds(2.5));
    }

    /// <summary>Ensures disposing the policy during an active execution does not fail the in-flight operation.</summary>
    [Fact]
    public async Task DisposeDoesNotBreakInFlightExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var policy = CreatePolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-g", timeProvider: TimeProvider.System);
        try
        {
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = new EnterReleaseGate(entered, release);

            var inFlight = policy.ExecuteAsync(
                gate,
                static async (g, ct) =>
                {
                    g.Entered.SetResult();
                    await g.Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                    return 7;
                },
                DefaultCancellationToken);

            await entered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

            release.SetResult();
            await policy.DisposeAsync();

            Assert.Equal(7, await inFlight);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, int>(policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), DefaultCancellationToken));
        }
        finally
        {
            await policy.DisposeAsync();
        }
    }

    /// <summary>
    /// Dispose racing <see cref="ServerCallPolicy.ExecuteAsync{TState,T}" /> must never surface an
    /// <see cref="ObjectDisposedException" /> raised from SemaphoreSlim internals: the
    /// claim-then-recheck ordering makes racing callers observe disposal through the policy's own
    /// post-enter check (or the drain gate) instead of a disposed concurrency semaphore. See issue #423.
    /// </summary>
    [Fact]
    public async Task DisposeRacingExecuteStaysClean()
    {
        const int rounds = 64;
        const int callersPerRound = 8;

        for (var round = 0; round < rounds; round++)
        {
            var policy = CreatePolicy(maxConcurrentPerPeer: 1, peer: $"peer-race-{round}", timeProvider: TimeProvider.System);
            try
            {
                using var drained = new ManualResetEventSlim(false);
                var faults = new ConcurrentQueue<string>();
                var callers = StartHammerCallers(policy, drained, faults, callersPerRound);

                // Spin-based phase smear: burning a round-dependent number of cycles before disposing
                // walks the dispose landing point through the callers' execute loop without depending
                // on coarse OS timer resolution, covering the whole claim window over time.
                Thread.SpinWait(((round % 64) + 1) * 256);
                await policy.DisposeAsync();
                drained.Set();
                foreach (var caller in callers)
                    await caller;

                Assert.False(faults.TryPeek(out var fault), $"SemaphoreSlim disposed fault escaped to a caller: {fault!}");
            }
            finally
            {
                await policy.DisposeAsync();
            }
        }
    }

    /// <summary>Ensures transient Http retries stop when maxAttempts is 1.</summary>
    [Fact]
    public async Task NoHttpRetryWhenMaxAttemptsIsOne()
    {
        await using var policy = CreatePolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-http-stop", timeProvider: TimeProvider.System);
        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException, int>(
            policy.ExecuteAsync(0, static (_, _) => ValueTask.FromException<int>(new HttpRequestException("boom")), DefaultCancellationToken));
        Assert.Contains("boom", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures non-retryable Rpc status codes stop without retry.</summary>
    [Fact]
    public async Task NoRetryForNonRetryableRpcStatus()
    {
        await using var policy = CreatePolicy(TimeSpan.FromSeconds(1), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-rpc-stop", timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                attempts,
                static (counter, cancellationToken) =>
                {
                    _ = cancellationToken;
                    _ = counter.Increment();
                    return ValueTask.FromException<int>(new RpcException(new Status(StatusCode.InvalidArgument, "bad")));
                },
                DefaultCancellationToken));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(1, attempts.Count);
    }

    /// <summary>Ensures caller cancellation stops retry flow and is not treated as per-attempt timeout.</summary>
    [Fact]
    public async Task CallerCancellationPreventsRetries()
    {
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(50), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-h", timeProvider: TimeProvider.System);
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = new InvocationCounter();

        var pending = policy.ExecuteAsync(
            new CancellationProbeState(entered, attempts),
            static async (s, token) =>
            {
                _ = s.Attempts.Increment();
                _ = s.Entered.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, token);
                return 1;
            },
            cts.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);
        await cts.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(pending);
        Assert.Equal(1, attempts.Count);
    }

    /// <summary>Ensures per-attempt timeout keeps existing retry behavior and can recover on a subsequent attempt.</summary>
    [Fact]
    public async Task PerAttemptTimeoutRetrySucceedsNextTry()
    {
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(25), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-i", timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();

        var value = await policy.ExecuteAsync(
            attempts,
            static async (counter, token) =>
            {
                var attempt = counter.Increment();
                if (attempt != 1)
                    return 42;
                await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                return 0;
            },
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, attempts.Count);
    }

    /// <summary>Ensures Unavailable RpcException retries and can succeed.</summary>
    [Fact]
    public async Task UnavailableRpcRetriedUntilSuccess()
    {
        var timeProvider = new FakeTimeProvider();
        await using var policy = CreatePolicy(
            TimeSpan.FromSeconds(1),
            2,
            TimeSpan.FromMilliseconds(5),
            TimeSpan.FromMilliseconds(5),
            peer: "peer-rpc-retry",
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

        timeProvider.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(9, await executeTask);
        Assert.Equal(2, attempts.Count);
    }

    /// <summary>Ensures a call queued behind the concurrency gate is rejected if drain begins before it starts executing.</summary>
    [Fact]
    public async Task QueuedCallRejectedWhenDrainStartsFirst()
    {
        var timeout = TimeSpan.FromSeconds(5);
        using var sink = new NodeMeasurementSink("Squirix");
        await using var policy = CreatePolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-f", timeProvider: TimeProvider.System);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainGate = new EnterReleaseGate(firstEntered, releaseFirst);

        var first = policy.ExecuteAsync(
            drainGate,
            static async (g, ct) =>
            {
                g.Entered.SetResult();
                await g.Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 1;
            },
            DefaultCancellationToken);

        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

        var queued = policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(2), DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(30), TimeProvider.System, DefaultCancellationToken);

        policy.BeginDrain();
        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(queued);
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-f"), ("scope", "policy")));
    }

    /// <summary>Ensures transient retries emit retry and backoff metrics.</summary>
    [Fact]
    public async Task RetryAndBackoffMetricsAreRecorded()
    {
        var timeProvider = new FakeTimeProvider();
        using var sink = new NodeMeasurementSink("Squirix");
        await using var policy = CreatePolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5), peer: "peer-d", timeProvider: timeProvider);
        var attempts = new InvocationCounter();

        var executeTask = policy.ExecuteAsync(
            attempts,
            static (counter, _) =>
            {
                var attempt = counter.Increment();
                return attempt == 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(42);
            },
            DefaultCancellationToken);

        while (attempts.Count < 1)
            await Task.Yield();

        timeProvider.Advance(TimeSpan.FromMinutes(1));

        var value = await executeTask;

        Assert.Equal(42, value);
        Assert.True(sink.HasEvent("squirix_call_policy_retries_total", ("peer", "peer-d"), ("reason", "http_request")));
        Assert.True(sink.HasEvent("squirix_call_policy_backoffs_total", ("peer", "peer-d"), ("scope", "policy")));
        Assert.True(sink.HasEvent("squirix_call_policy_queue_wait_seconds", ("peer", "peer-d")));
    }

    /// <summary>Ensures timeout metrics record deadline-budget exhaustion as a separate category.</summary>
    [Fact]
    public async Task TimeoutMetricsRecordedAsOwnCategory()
    {
        using var sink = new NodeMeasurementSink("Squirix");
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(100), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-b", timeProvider: TimeProvider.System);
        using var deadline = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(35));
        _ = Assert.NotNull(ServerRpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                0,
                static async (_, token) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                    return 1;
                },
                DefaultCancellationToken));
        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);

        Assert.True(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", "peer-b"), ("scope", "overall"), ("kind", "deadline_budget")));
    }

    private static ServerCallPolicy CreatePolicy(
        TimeSpan? timeoutPerAttempt = null,
        int maxAttempts = 3,
        TimeSpan? baseBackoff = null,
        TimeSpan? maxBackoff = null,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        TimeProvider? timeProvider = null) => new(timeoutPerAttempt, maxAttempts, baseBackoff, maxBackoff, maxConcurrentPerPeer, peer, timeProvider ?? TimeProvider.System);

    private static Task[] StartHammerCallers(ServerCallPolicy policy, ManualResetEventSlim drained, ConcurrentQueue<string> faults, int count)
    {
        Task StartCallerAsync()
        {
            return Task.Factory.StartNew(
                () => HammerExecuteUntilDisposedAsync(policy, drained, faults),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }

        var callers = new Task[count];
        for (var i = 0; i < count; i++)
            callers[i] = StartCallerAsync();

        return callers;
    }

    private static async Task HammerExecuteUntilDisposedAsync(ServerCallPolicy policy, ManualResetEventSlim drained, ConcurrentQueue<string> faults)
    {
        while (!drained.IsSet)
        {
            try
            {
                _ = await policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), CancellationToken.None);
            }
            catch (Exception ex) when (ex is RpcException or OperationCanceledException)
            {
                return; // Drain rejection or shutdown cancellation - legitimate outcome.
            }
            catch (ObjectDisposedException disposed)
            {
                // THE regression signature: use-after-dispose of the concurrency semaphore.
                // Classification keys on ObjectDisposedException.ObjectName instead of stack-trace
                // text: both policies throw their post-enter check via ThrowIf(..., this), which
                // reports the policy type name, so only an ObjectName identifying SemaphoreSlim
                // counts as a fault.
                if (string.Equals(disposed.ObjectName, typeof(SemaphoreSlim).Name, StringComparison.Ordinal))
                    faults.Enqueue(disposed.ToString());

                return;
            }
        }
    }

    [Immutable]
    private sealed class CancellationProbeState
    {
        internal CancellationProbeState(TaskCompletionSource entered, InvocationCounter attempts)
        {
            Entered = entered;
            Attempts = attempts;
        }

        internal InvocationCounter Attempts { get; }

        internal TaskCompletionSource Entered { get; }
    }

    [Immutable]
    private sealed class ConcurrencySyncState
    {
        internal ConcurrencySyncState(TaskCompletionSource firstEntered, TaskCompletionSource releaseFirst, PeakCounter peak)
        {
            FirstEntered = firstEntered;
            Peak = peak;
            ReleaseFirst = releaseFirst;
        }

        internal TaskCompletionSource FirstEntered { get; }

        internal PeakCounter Peak { get; }

        internal TaskCompletionSource ReleaseFirst { get; }

        internal RunningCounter Running { get; } = new();
    }

    [Immutable]
    private sealed class EnterReleaseGate
    {
        internal EnterReleaseGate(TaskCompletionSource entered, TaskCompletionSource release)
        {
            Entered = entered;
            Release = release;
        }

        internal TaskCompletionSource Entered { get; }

        internal TaskCompletionSource Release { get; }
    }

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }

    private sealed class PeakCounter
    {
        private int _peak;

        internal int Peak => Volatile.Read(ref _peak);

        internal void Record(int value)
        {
            var current = Volatile.Read(ref _peak);
            while (value > current)
            {
                var observed = Interlocked.CompareExchange(ref _peak, value, current);
                if (observed == current)
                    return;

                current = observed;
            }
        }
    }

    private sealed class RunningCounter
    {
        private int _count;

        internal int Decrement() => Interlocked.Decrement(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
