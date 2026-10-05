using System;
using System.Collections.Concurrent;
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

/// <summary>Unit tests for deadline-aware retry and timeout handling in <see cref="ServerCallPolicy" />.</summary>
[Immutable]
public sealed class NodeCallPolicyTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Ensures the ambient request deadline caps the overall retry budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AmbientDeadlineCapsOverallRetryBudget(CancellationToken cancellationToken)
    {
        var timeouts = new CallPolicyTimeouts(TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5));
        await using var policy = CreatePolicy(timeouts, 5, peer: "peer-a", timeProvider: TimeProvider.System);
        using var deadline = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(50), TimeProvider.System);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                0,
                static async (_, token) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                    return 1;
                },
                cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>Ensures draining a policy rejects new peer RPC execution immediately.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginDrainRejectsNewCalls(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        await using var policy = CreatePolicy(peer: "peer-c", timeProvider: TimeProvider.System, meter: meter);
        policy.BeginDrain();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-c"), ("scope", "policy"))).IsTrue();
    }

    /// <summary>Ensures caller cancellation stops retry flow and is not treated as per-attempt timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancellationPreventsRetries(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromMilliseconds(50), TimeSpan.Zero, TimeSpan.Zero),
            peer: "peer-h",
            timeProvider: TimeProvider.System);
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

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await cts.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(pending);
        _ = await Assert.That(attempts.Count).IsEqualTo(1);
    }

    /// <summary>Ensures the write-ahead ambiguous outcome is not retried: the caller stops and surfaces it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitOutcomeUnknownStopsRetries(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(peer: "peer-unknown", timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                attempts,
                static (counter, cancellationToken) =>
                {
                    _ = cancellationToken;
                    _ = counter.Increment();
                    return ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)));
                },
                cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(attempts.Count).IsEqualTo(1);
    }

    /// <summary>Ensures a call that finds no free per-peer permit is refused at once without running or waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrencyCapRejectsWithoutWaiting(CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(5);
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        await using var policy = CreatePolicy(new CallPolicyTimeouts(timeout), maxConcurrentPerPeer: 1, peer: "peer-e", timeProvider: TimeProvider.System, meter: meter);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new EnterReleaseGate(firstEntered, releaseFirst);
        var rejectedRuns = new InvocationCounter();

        var first = policy.ExecuteAsync(
            gate,
            static async (g, ct) =>
            {
                g.Entered.SetResult();
                await g.Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 1;
            },
            cancellationToken);
        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, cancellationToken);

        var second = policy.ExecuteAsync(
            rejectedRuns,
            static (counter, __) =>
            {
                _ = counter.Increment();
                return ValueTask.FromResult(2);
            },
            cancellationToken);

        _ = await Assert.That(second.IsCompleted).IsTrue();
        var ex = await NodeAsyncAssert.ThrowsAsync<SquirixException, int>(second);
        _ = await Assert.That(ex.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(ex.Message).Contains("peer_busy");
        _ = await Assert.That(rejectedRuns.Count).IsEqualTo(0);
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_busy_rejects_total", ("peer", "peer-e"), ("scope", "policy"))).IsTrue();

        releaseFirst.SetResult();
        _ = await Assert.That(await first).IsEqualTo(1);

        var third = await policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(3), cancellationToken);
        _ = await Assert.That(third).IsEqualTo(3);
    }

    /// <summary>Ensures a drain that begins while the permit is held is reported as a drain, not as a busy peer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyPeerDuringDrainReportsDrain(CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(5);
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        await using var policy = CreatePolicy(new CallPolicyTimeouts(timeout), maxConcurrentPerPeer: 1, peer: "peer-f", timeProvider: TimeProvider.System, meter: meter);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new EnterReleaseGate(firstEntered, releaseFirst);

        var first = policy.ExecuteAsync(
            gate,
            static async (g, ct) =>
            {
                g.Entered.SetResult();
                await g.Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 1;
            },
            cancellationToken);
        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, cancellationToken);

        policy.BeginDrain();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(2), cancellationToken));
        releaseFirst.SetResult();

        _ = await Assert.That(await first).IsEqualTo(1);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-f"), ("scope", "policy"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_busy_rejects_total", ("peer", "peer-f"), ("scope", "policy"))).IsFalse();
    }

    /// <summary>Ensures outbound call options inherit the ambient deadline budget.</summary>
    [Test]
    public async Task DeadlineContextComputesCallDeadline()
    {
        using var scope = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(2), TimeProvider.System);

        var effective = ServerRpcDeadlineContext.EffectiveDeadline(DateTime.UtcNow.AddSeconds(5));

        _ = await Assert.That(effective).IsNotNull();
        _ = await Assert.That(effective <= DateTime.UtcNow.AddSeconds(2.5)).IsTrue();
    }

    /// <summary>Ensures disposing the policy during an active execution does not fail the in-flight operation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDoesNotBreakInFlightExecution(CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(5);
        var policy = CreatePolicy(new CallPolicyTimeouts(timeout), maxConcurrentPerPeer: 1, peer: "peer-g", timeProvider: TimeProvider.System);
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
                cancellationToken);

            await entered.Task.WaitAsync(timeout, TimeProvider.System, cancellationToken);

            release.SetResult();
            await policy.DisposeAsync();

            _ = await Assert.That(await inFlight).IsEqualTo(7);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, int>(policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), cancellationToken));
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
    /// post-enter check (or the drain gate) instead of a disposed concurrency semaphore.
    /// </summary>
    [Test]
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

                // Await every caller reaching its hammer loop (and so its first ExecuteAsync) before
                // disposing, so a busy runner cannot dispose before any caller enters the race.
                Task[] callers;
                foreach (var signal in StartHammerCallers(policy, drained, faults, callersPerRound, out callers))
                    await signal;

                // Spin-based phase smear: burning a round-dependent number of cycles before disposing
                // walks the disposal landing point through the callers' execute loop without depending
                // on coarse OS timer resolution, covering the whole claim window over time.
                Thread.SpinWait(((round % 64) + 1) * 256);
                await policy.DisposeAsync();
                drained.Set();

                foreach (var caller in callers)
                    await caller;

                _ = await Assert.That(faults.TryPeek(out var fault)).IsFalse().Because($"SemaphoreSlim disposed fault escaped to a caller: {fault}");
            }
            finally
            {
                await policy.DisposeAsync();
            }
        }
    }

    /// <summary>Ensures transient Http retries stop when maxAttempts is 1.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoHttpRetryWhenMaxAttemptsIsOne(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.Zero, TimeSpan.Zero),
            1,
            peer: "peer-http-stop",
            timeProvider: TimeProvider.System);
        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException, int>(
            policy.ExecuteAsync(0, static (_, _) => ValueTask.FromException<int>(new HttpRequestException("boom")), cancellationToken));
        _ = await Assert.That(ex.Message).Contains("boom", StringComparison.Ordinal);
    }

    /// <summary>Ensures non-retryable Rpc status codes stop without a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoRetryForNonRetryableRpcStatus(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.Zero, TimeSpan.Zero),
            peer: "peer-rpc-stop",
            timeProvider: TimeProvider.System);
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
                cancellationToken));
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(attempts.Count).IsEqualTo(1);
    }

    /// <summary>Ensures an admission refusal by the peer passes through without a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResourceExhaustedIsNotRetried(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.Zero, TimeSpan.Zero),
            peer: "peer-refusal",
            timeProvider: TimeProvider.System);
        var attempts = new InvocationCounter();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                attempts,
                static (counter, cancellationToken) =>
                {
                    _ = cancellationToken;
                    _ = counter.Increment();
                    return ValueTask.FromException<int>(new RpcException(new Status(StatusCode.ResourceExhausted, "Server is overloaded.")));
                },
                cancellationToken));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(attempts.Count).IsEqualTo(1);
    }

    /// <summary>Ensures per-attempt timeout keeps existing retry behavior and can recover on a subsequent attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerAttemptTimeoutRetrySucceedsNextTry(CancellationToken cancellationToken)
    {
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromMilliseconds(25), TimeSpan.Zero, TimeSpan.Zero),
            2,
            peer: "peer-i",
            timeProvider: TimeProvider.System);
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
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(attempts.Count).IsEqualTo(2);
    }

    /// <summary>Ensures transient retries emit retry and backoff metrics.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryAndBackoffMetricsAreRecorded(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5)),
            2,
            peer: "peer-d",
            timeProvider: timeProvider,
            meter: meter);
        var attempts = new InvocationCounter();

        var executeTask = policy.ExecuteAsync(
            attempts,
            static (counter, _) =>
            {
                var attempt = counter.Increment();
                return attempt == 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(42);
            },
            cancellationToken);

        while (attempts.Count < 1)
            await Task.Yield();

        timeProvider.Advance(TimeSpan.FromMinutes(1));

        var value = await executeTask;

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_retries_total", ("peer", "peer-d"), ("reason", "http_request"))).IsTrue();
        _ = await Assert.That(sink.HasEvent("squirix_call_policy_backoffs_total", ("peer", "peer-d"), ("scope", "policy"))).IsTrue();
    }

    /// <summary>Ensures timeout metrics record deadline-budget exhaustion as a separate category.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutMetricsRecordedAsOwnCategory(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromMilliseconds(100), TimeSpan.Zero, TimeSpan.Zero),
            2,
            peer: "peer-b",
            timeProvider: TimeProvider.System,
            meter: meter);
        using var deadline = ServerRpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(35), TimeProvider.System);
        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsNotNull();

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                0,
                static async (_, token) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                    return 1;
                },
                cancellationToken));
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);

        _ = await Assert.That(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", "peer-b"), ("scope", "overall"), ("kind", "deadline_budget"))).IsTrue();
    }

    /// <summary>Ensures Unavailable RpcException retries and can succeed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnavailableRpcRetriedUntilSuccess(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        await using var policy = CreatePolicy(
            new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5)),
            2,
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
            cancellationToken);

        while (attempts.Count < 1)
            await Task.Yield();

        timeProvider.Advance(TimeSpan.FromMinutes(1));
        _ = await Assert.That(await executeTask).IsEqualTo(9);
        _ = await Assert.That(attempts.Count).IsEqualTo(2);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static async Task HammerExecuteUntilDisposedAsync(
        ServerCallPolicy policy,
        ManualResetEventSlim drained,
        ConcurrentQueue<string> faults,
        TaskCompletionSource readySignal)
    {
        while (!drained.IsSet)
        {
            try
            {
                _ = await policy.ExecuteAsync(0, static (_, _) => ValueTask.FromResult(1), CancellationToken.None);
                _ = readySignal.TrySetResult();
            }
            catch (Exception ex) when (ex is RpcException or OperationCanceledException)
            {
                return; // Drain rejection or shutdown cancellation - legitimate outcome.
            }
            catch (SquirixException)
            {
                // The single permit is held by another hammer caller: a busy refusal is a legitimate outcome here.
                _ = readySignal.TrySetResult();
            }
            catch (ObjectDisposedException disposed)
            {
                // THE regression signature: use-after-dispose of the concurrency semaphore.
                // Classification keys on ObjectDisposedException.ObjectName instead of stack-trace
                // text: both policies throw their post-enter check via ThrowIf(..., this), which
                // reports the policy type name, so only an ObjectName identifying SemaphoreSlim
                // counts as a fault.
                if (string.Equals(disposed.ObjectName, nameof(SemaphoreSlim), StringComparison.Ordinal))
                    faults.Enqueue(disposed.ToString());

                return;
            }
        }
    }

    private static Task[] StartHammerCallers(ServerCallPolicy policy, ManualResetEventSlim drained, ConcurrentQueue<string> faults, int count, out Task[] callers)
    {
        var started = new Task[count];
        callers = new Task[count];
        for (var i = 0; i < count; i++)
        {
            var readySignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            started[i] = readySignal.Task;
            callers[i] = StartCallerAsync(readySignal);
        }

        return started;

        Task StartCallerAsync(TaskCompletionSource readySignal)
        {
            return Task.Factory.StartNew(
                () => HammerExecuteUntilDisposedAsync(policy, drained, faults, readySignal),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }
    }

    private ServerCallPolicy CreatePolicy(
        CallPolicyTimeouts? timeouts = null,
        int maxAttempts = 3,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        TimeProvider? timeProvider = null,
        Meter? meter = null) => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter ?? _testMeter), new ServerRpcTimeoutMetrics(meter ?? _testMeter)),
        maxAttempts,
        maxConcurrentPerPeer,
        peer,
        timeProvider ?? TimeProvider.System,
        timeouts ?? new CallPolicyTimeouts());

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
}
