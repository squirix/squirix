using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal.Cluster.Reliability;
using Xunit;

namespace Squirix.UnitTests.Cluster;

/// <summary>Covers client <see cref="CallPolicy" /> Map* failure classification paths.</summary>
public sealed class CallPolicyTests
{
    /// <summary>Retries HttpRequestException then succeeds.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesHttpRequestException()
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

    /// <summary>Stops on HttpRequestException when maxAttempts is 1.</summary>
    [Fact]
    public async Task ExecuteAsyncStopsHttpWhenMaxAttemptsIsOne()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-http-stop");
        _ = await Assert.ThrowsAsync<HttpRequestException>(() => policy.ExecuteAsync(
            static (_, _) => ValueTask.FromException<int>(new HttpRequestException("boom")),
            0,
            CancellationToken.None).AsTask());
    }

    /// <summary>Stops on non-retryable Rpc status.</summary>
    [Fact]
    public async Task ExecuteAsyncStopsNonRetryableRpc()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-stop");
        var box = new IntBox();
        var ex = await Assert.ThrowsAsync<RpcException>(() => policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                _ = counter.Increment();
                return ValueTask.FromException<int>(new RpcException(new Status(StatusCode.InvalidArgument, "bad")));
            },
            box,
            CancellationToken.None).AsTask());
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(1, box.Count);
    }

    /// <summary>Retries Unavailable RpcException.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesUnavailableRpc()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-retry");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n is 1
                    ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, "down")))
                    : new ValueTask<int>(8);
            },
            box,
            CancellationToken.None);

        Assert.Equal(8, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Retries DeadlineExceeded RpcException.</summary>
    [Fact]
    public async Task ExecuteAsyncRetriesDeadlineExceededRpc()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-deadline");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n is 1
                    ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow")))
                    : new ValueTask<int>(4);
            },
            box,
            CancellationToken.None);

        Assert.Equal(4, value);
        Assert.Equal(2, box.Count);
    }

    private sealed class IntBox
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }
}
