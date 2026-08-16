using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Cluster;

/// <summary>Covers client <see cref="CallPolicy" /> Map* failure classification paths.</summary>
[Immutable]
public sealed class CallPolicyTests
{
    /// <summary>Rejects new calls after BeginDrain.</summary>
    [Fact]
    public async Task BeginDrainRejectsNewCallsAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-drain");
        policy.BeginDrain();

        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(static (_, _) => ValueTask.FromResult(1), 0, CancellationToken.None));
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
    }

    /// <summary>Retries DeadlineExceeded RpcException.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesDeadlineExceededRpcAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-deadline");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n is 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow"))) : new ValueTask<int>(4);
            },
            box,
            CancellationToken.None);

        Assert.Equal(4, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Retries HttpRequestException then succeeds.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesHttpRequestExceptionAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-http");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n is 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(3);
            },
            box,
            CancellationToken.None);

        Assert.Equal(3, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Retries Unavailable RpcException.</summary>
    [Fact]
    public async Task ExecuteRetriesUnavailableRpcAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-retry");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n is 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, "down"))) : new ValueTask<int>(8);
            },
            box,
            CancellationToken.None);

        Assert.Equal(8, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Stops on HttpRequestException when maxAttempts is 1.</summary>
    [Fact]
    public async Task ExecuteAsyncStopsHttpWhenMaxAttemptsIsOneAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-http-stop");
        _ = await AsyncAssert.ThrowsAsync<HttpRequestException, int>(
            policy.ExecuteAsync(static (_, _) => ValueTask.FromException<int>(new HttpRequestException("boom")), 0, CancellationToken.None));
    }

    /// <summary>Stops on non-retryable Rpc status.</summary>
    [Fact]
    public async Task ExecuteAsyncStopsNonRetryableRpcAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-stop");
        var box = new IntBox();
        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                static (counter, cancellationToken) =>
                {
                    _ = cancellationToken;
                    _ = counter.Increment();
                    return ValueTask.FromException<int>(new RpcException(new Status(StatusCode.InvalidArgument, "bad")));
                },
                box,
                CancellationToken.None));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(1, box.Count);
    }

    /// <summary>Rejects a call queued behind the concurrency gate when drain begins before execution.</summary>
    [Fact]
    public async Task ExecuteAsyncQueuedCallRejectedOnDrainAsync()
    {
        var timeout = TimeSpan.FromSeconds(5);
        await using var policy = new CallPolicy(timeout, 1, TimeSpan.Zero, TimeSpan.Zero, 1, "c-drain-queue");
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new EnterReleaseGate(firstEntered, releaseFirst);

        var first = policy.ExecuteAsync(
            static async (g, ct) =>
            {
                g.Entered.SetResult();
                await g.Release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 1;
            },
            gate,
            CancellationToken.None);

        await firstEntered.Task.WaitAsync(timeout, TimeProvider.System, CancellationToken.None);

        var queued = policy.ExecuteAsync(static (_, _) => ValueTask.FromResult(2), 0, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(30), TimeProvider.System, CancellationToken.None);

        policy.BeginDrain();
        releaseFirst.SetResult();

        Assert.Equal(1, await first);
        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(queued);
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
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

    private sealed class IntBox
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
