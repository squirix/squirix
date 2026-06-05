using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Node.Cluster.Reliability;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Cluster;

/// <summary>
/// Unit tests for deadline-aware retry and timeout handling in <see cref="CallPolicy" />.
/// </summary>
public sealed class CallPolicyTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures the ambient request deadline caps the overall retry budget.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task AmbientDeadlineCapsOverallRetryBudget()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(5), 5, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5), peer: "peer-a");
        using var deadline = RpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(50));

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await policy.ExecuteAsync(
                static async token =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    return 1;
                },
                DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);
    }

    /// <summary>
    /// Ensures draining a policy rejects new peer RPC execution immediately.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task BeginDrainRejectsNewCalls()
    {
        using var sink = new MeasurementSink("Squirix");
        await using var policy = new CallPolicy(peer: "peer-c");
        policy.BeginDrain();

        var ex = await Assert.ThrowsAsync<RpcException>(async () => { _ = await policy.ExecuteAsync(static _ => ValueTask.FromResult(1), DefaultCancellationToken); });

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-c"), ("scope", "policy")));
    }

    /// <summary>
    /// Verifies that retry reason classification does not allocate for gRPC status codes on the hot path.
    /// </summary>
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

    /// <summary>
    /// Ensures the per-peer concurrency cap does not allow more concurrent executions than configured.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ConcurrencyCapSerializesExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        await using var policy = new CallPolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-e");
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var maxRunning = 0;

        var first = policy.ExecuteAsync(
            async ct =>
            {
                var nowRunning = Interlocked.Increment(ref running);
                maxRunning = Math.Max(maxRunning, nowRunning);
                try
                {
                    firstEntered.SetResult();
                    await releaseFirst.Task.WaitAsync(ct);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref running);
                }

                return 1;
            },
            DefaultCancellationToken);
        await firstEntered.Task.WaitAsync(timeout, DefaultCancellationToken);

        var second = policy.ExecuteAsync(
            __ =>
            {
                var nowRunning = Interlocked.Increment(ref running);
                maxRunning = Math.Max(maxRunning, nowRunning);
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
        await Task.Delay(30, DefaultCancellationToken);
        Assert.False(second.IsCompleted);

        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        Assert.Equal(2, await second);
        Assert.Equal(1, maxRunning);
    }

    /// <summary>
    /// Ensures outbound call options inherit the ambient deadline budget.
    /// </summary>
    [Fact]
    public void DeadlineContextComputesEffectiveCallDeadline()
    {
        using var scope = RpcDeadlineContext.Push(DateTime.UtcNow.AddSeconds(2));

        var effective = RpcDeadlineContext.EffectiveDeadline(DateTime.UtcNow.AddSeconds(5));

        _ = Assert.NotNull(effective);
        Assert.True(effective <= DateTime.UtcNow.AddSeconds(2.5));
    }

    /// <summary>
    /// Ensures disposing the policy during an active execution does not fail the in-flight operation.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task DisposeDoesNotBreakInFlightExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var policy = new CallPolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-g");
        try
        {
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var inFlight = policy.ExecuteAsync(
                async ct =>
                {
                    entered.SetResult();
                    await release.Task.WaitAsync(ct);
                    return 7;
                },
                DefaultCancellationToken);

            await entered.Task.WaitAsync(timeout, DefaultCancellationToken);

            var disposeTask = policy.DisposeAsync().AsTask();
            release.SetResult();
            await disposeTask;

            Assert.Equal(7, await inFlight);
            _ = await Assert.ThrowsAsync<ObjectDisposedException>(async () => await policy.ExecuteAsync(static _ => ValueTask.FromResult(1), DefaultCancellationToken));
        }
        finally
        {
            await policy.DisposeAsync();
        }
    }

    /// <summary>
    /// Ensures caller cancellation stops retry flow and is not treated as per-attempt timeout.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ExecuteAsyncDoesNotRetryWhenCallerCancellationWins()
    {
        await using var policy = new CallPolicy(TimeSpan.FromMilliseconds(50), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-h");
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;

        var pending = policy.ExecuteAsync(
            async token =>
            {
                _ = Interlocked.Increment(ref attempts);
                _ = entered.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return 1;
            },
            cts.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), DefaultCancellationToken);
        await cts.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(pending.AsTask);
        Assert.Equal(1, attempts);
    }

    /// <summary>
    /// Ensures per-attempt timeout keeps existing retry behavior and can recover on a subsequent attempt.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ExecuteAsyncRetriesPerAttemptTimeoutAndSucceedsOnNextAttempt()
    {
        await using var policy = new CallPolicy(TimeSpan.FromMilliseconds(25), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-i");
        var attempts = 0;

        var value = await policy.ExecuteAsync(
            async token =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt != 1)
                    return 42;
                await Task.Delay(TimeSpan.FromSeconds(1), token);
                return 0;
            },
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, attempts);
    }

    /// <summary>
    /// Ensures a call queued behind the concurrency gate is rejected if drain begins before it starts executing.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task QueuedCallIsRejectedIfDrainBeginsBeforeExecution()
    {
        var timeout = TimeSpan.FromSeconds(5);
        using var sink = new MeasurementSink("Squirix");
        await using var policy = new CallPolicy(timeout, maxConcurrentPerPeer: 1, peer: "peer-f");
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = policy.ExecuteAsync(
            async ct =>
            {
                firstEntered.SetResult();
                await releaseFirst.Task.WaitAsync(ct);
                return 1;
            },
            DefaultCancellationToken);

        await firstEntered.Task.WaitAsync(timeout, DefaultCancellationToken);

        var queued = policy.ExecuteAsync(static _ => ValueTask.FromResult(2), DefaultCancellationToken);
        await Task.Delay(30, DefaultCancellationToken);

        policy.BeginDrain();
        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        var ex = await Assert.ThrowsAsync<RpcException>(async () => await queued);
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.True(sink.HasEvent("squirix_call_policy_drain_rejects_total", ("peer", "peer-f"), ("scope", "policy")));
    }

    /// <summary>
    /// Ensures transient retries emit retry and backoff metrics.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RetryAndBackoffMetricsAreRecorded()
    {
        using var sink = new MeasurementSink("Squirix");
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5), peer: "peer-d");
        var attempts = 0;

        var value = await policy.ExecuteAsync(
            _ =>
            {
                attempts++;
                return attempts == 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(42);
            },
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.True(sink.HasEvent("squirix_call_policy_retries_total", ("peer", "peer-d"), ("reason", "http_request")));
        Assert.True(sink.HasEvent("squirix_call_policy_backoffs_total", ("peer", "peer-d"), ("scope", "policy")));
        Assert.True(sink.HasEvent("squirix_call_policy_queue_wait_seconds", ("peer", "peer-d")));
    }

    /// <summary>
    /// Ensures timeout metrics record deadline-budget exhaustion as a separate category.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task TimeoutMetricsAreRecordedAsFirstClassCategory()
    {
        using var sink = new MeasurementSink("Squirix");
        await using var policy = new CallPolicy(TimeSpan.FromMilliseconds(100), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "peer-b");
        using var deadline = RpcDeadlineContext.Push(DateTime.UtcNow.AddMilliseconds(35));
        _ = Assert.NotNull(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await policy.ExecuteAsync(
                static async token =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    return 1;
                },
                DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.DeadlineExceeded, ex.StatusCode);

        Assert.True(sink.HasEvent("squirix_rpc_timeouts_total", ("peer", "peer-b"), ("scope", "overall"), ("kind", "deadline_budget")));
    }
}
