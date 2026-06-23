using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.Testing;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>
/// Unit tests for deadline-aware retry and timeout handling in <see cref="CallPolicy" />.
/// </summary>
public sealed class CallPolicyTests : UnitTestBase
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
        using var deadline = RpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(50));

        var ex = await Assert.ThrowsAsync<RpcException>(() => policy.ExecuteAsync(
            static async token =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                return 1;
            },
            DefaultCancellationToken).AsTask());

        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);
    }

    /// <summary>Ensures draining a policy rejects new peer RPC execution immediately.</summary>
    [Fact]
    public async Task BeginDrainRejectsNewCalls()
    {
        using var sink = new MeasurementSink("Squirix");
        await using var policy = CreatePolicy(peer: "peer-c", timeProvider: TimeProvider.System);
        policy.BeginDrain();

        var ex = await Assert.ThrowsAsync<RpcException>(() => policy.ExecuteAsync(static _ => ValueTask.FromResult(1), DefaultCancellationToken).AsTask());

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-c"), ("scope", "policy")));
    }

    /// <summary>Verifies that retry reason classification does not allocate for gRPC status codes on the hot path.</summary>
    [Fact]
    public void ClassifyRetryReasonDoesNotAllocate()
    {
        var ex = new RpcException(new Status(StatusCode.DeadlineExceeded, "boom"));

        _ = CallPolicy.ClassifyRetryReason(ex);

        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            for (var i = 0; i < 10_000; i++)
                _ = CallPolicy.ClassifyRetryReason(ex);
        });

        Assert.Equal(0, allocated);
    }

    /// <summary>Ensures the per-peer concurrency cap does not allow more concurrent executions than configured.</summary>
    [Fact]
    public async Task ConcurrencyCapSerializesExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        await using var policy = CreatePolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-e", timeProvider: TimeProvider.System);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var peakRunning = new PeakCounter();

        var first = policy.ExecuteAsync(
            async ct =>
            {
                peakRunning.Record(Interlocked.Increment(ref running));
                try
                {
                    firstEntered.SetResult();
                    await releaseFirst.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref running);
                }

                return 1;
            },
            DefaultCancellationToken);
        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

        var second = policy.ExecuteAsync(
            __ =>
            {
                peakRunning.Record(Interlocked.Increment(ref running));
                try
                {
                    return ValueTask.FromResult(2);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref running);
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
    public void DeadlineContextComputesEffectiveCallDeadline()
    {
        using var scope = RpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(2));

        var effective = RpcDeadlineContext.EffectiveDeadline(DateTime.UtcNow.AddSeconds(5));

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

            var inFlight = policy.ExecuteAsync(
                async ct =>
                {
                    entered.SetResult();
                    await release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                    return 7;
                },
                DefaultCancellationToken);

            await entered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

            release.SetResult();
            await policy.DisposeAsync();

            Assert.Equal(7, await inFlight);
            _ = await Assert.ThrowsAsync<ObjectDisposedException>(() => policy.ExecuteAsync(static _ => ValueTask.FromResult(1), DefaultCancellationToken).AsTask());
        }
        finally
        {
            await policy.DisposeAsync();
        }
    }

    /// <summary>Ensures caller cancellation stops retry flow and is not treated as per-attempt timeout.</summary>
    [Fact]
    public async Task ExecuteAsyncDoesNotRetryWhenCallerCancellationWins()
    {
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(50), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-h", timeProvider: TimeProvider.System);
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;

        var pending = policy.ExecuteAsync(
            async token =>
            {
                _ = Interlocked.Increment(ref attempts);
                _ = entered.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, token);
                return 1;
            },
            cts.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);
        await cts.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(pending.AsTask);
        Assert.Equal(1, attempts);
    }

    /// <summary>Ensures per-attempt timeout keeps existing retry behavior and can recover on a subsequent attempt.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesPerAttemptTimeoutAndSucceedsOnNextAttempt()
    {
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(25), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-i", timeProvider: TimeProvider.System);
        var attempts = 0;

        var value = await policy.ExecuteAsync(
            async token =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt is not 1)
                    return 42;
                await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                return 0;
            },
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, attempts);
    }

    /// <summary>Ensures a call queued behind the concurrency gate is rejected if drain begins before it starts executing.</summary>
    [Fact]
    public async Task QueuedCallIsRejectedIfDrainBeginsBeforeExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        using var sink = new MeasurementSink("Squirix");
        await using var policy = CreatePolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-f", timeProvider: TimeProvider.System);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = policy.ExecuteAsync(
            async ct =>
            {
                firstEntered.SetResult();
                await releaseFirst.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 1;
            },
            DefaultCancellationToken);

        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, DefaultCancellationToken);

        var queued = policy.ExecuteAsync(static _ => ValueTask.FromResult(2), DefaultCancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(30), TimeProvider.System, DefaultCancellationToken);

        policy.BeginDrain();
        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        var ex = await Assert.ThrowsAsync<RpcException>(async () => { _ = await queued; });
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-f"), ("scope", "policy")));
    }

    /// <summary>Ensures transient retries emit retry and backoff metrics.</summary>
    [Fact]
    public async Task RetryAndBackoffMetricsAreRecorded()
    {
        var timeProvider = new FakeTimeProvider();
        using var sink = new MeasurementSink("Squirix");
        await using var policy = CreatePolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5), peer: "peer-d", timeProvider: timeProvider);
        var attempts = new InvocationCounter();

        var executeTask = policy.ExecuteAsync(
            _ =>
            {
                var attempt = attempts.Increment();
                return attempt is 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(42);
            },
            DefaultCancellationToken);

        while (attempts.Value < 1)
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
    public async Task TimeoutMetricsAreRecordedAsFirstClassCategory()
    {
        using var sink = new MeasurementSink("Squirix");
        await using var policy = CreatePolicy(TimeSpan.FromMilliseconds(100), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-b", timeProvider: TimeProvider.System);
        using var deadline = RpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(35));
        _ = Assert.NotNull(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        var ex = await Assert.ThrowsAsync<RpcException>(() => policy.ExecuteAsync(
            static async token =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1), TimeProvider.System, token);
                return 1;
            },
            DefaultCancellationToken).AsTask());
        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);

        Assert.True(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", "peer-b"), ("scope", "overall"), ("kind", "deadline_budget")));
    }

    private static CallPolicy CreatePolicy(
        TimeSpan? timeoutPerAttempt = null,
        int maxAttempts = 3,
        TimeSpan? baseBackoff = null,
        TimeSpan? maxBackoff = null,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        TimeProvider? timeProvider = null) => new(timeoutPerAttempt, maxAttempts, baseBackoff, maxBackoff, maxConcurrentPerPeer, peer, timeProvider ?? TimeProvider.System);

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Value => Volatile.Read(ref _count);

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
}
