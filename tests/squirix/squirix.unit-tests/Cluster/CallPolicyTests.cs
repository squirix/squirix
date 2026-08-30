using System;
using System.Collections.Concurrent;
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

    /// <summary>
    /// Dispose racing <see cref="CallPolicy.ExecuteAsync{TState,T}" /> must never surface an
    /// <see cref="ObjectDisposedException" /> raised from SemaphoreSlim internals: the
    /// claim-then-recheck ordering makes racing callers observe disposal through the policy's own
    /// post-enter check (or the drain gate) instead of a disposed concurrency semaphore.
    /// Mirrors the server-side regression test for issue #423.
    /// </summary>
    [Fact]
    public async Task DisposeRacingExecuteStaysClean()
    {
        const int rounds = 64;
        const int callersPerRound = 8;

        for (var round = 0; round < rounds; round++)
        {
            await using var policy = new CallPolicy(TimeSpan.FromSeconds(5), 1, TimeSpan.Zero, TimeSpan.Zero, 1, $"c-race-{round}");
            using var drained = new ManualResetEventSlim(false);
            var faults = new ConcurrentQueue<string>();

            // Await every caller reaching its hammer loop (and so its first ExecuteAsync) before
            // disposing, so a busy runner cannot dispose before any caller enters the race.
            Task[] callers;
            foreach (var signal in StartHammerCallers(policy, drained, faults, callersPerRound, out callers))
                await signal;

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
                return n == 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, "down"))) : new ValueTask<int>(8);
            },
            box,
            CancellationToken.None);

        Assert.Equal(8, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Rejects a call queued behind the concurrency gate when drain begins before execution.</summary>
    [Fact]
    public async Task QueuedCallRejectedOnDrainAsync()
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

    /// <summary>Retries DeadlineExceeded RpcException.</summary>
    [Fact]
    public async Task RetriesDeadlineExceededRpcAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-rpc-deadline");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n == 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow"))) : new ValueTask<int>(4);
            },
            box,
            CancellationToken.None);

        Assert.Equal(4, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Retries HttpRequestException then succeeds.</summary>
    [Fact]
    public async Task RetriesHttpRequestExceptionAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 2, TimeSpan.Zero, TimeSpan.Zero, peer: "c-http");
        var box = new IntBox();
        var value = await policy.ExecuteAsync(
            static (counter, cancellationToken) =>
            {
                _ = cancellationToken;
                var n = counter.Increment();
                return n == 1 ? ValueTask.FromException<int>(new HttpRequestException("boom")) : new ValueTask<int>(3);
            },
            box,
            CancellationToken.None);

        Assert.Equal(3, value);
        Assert.Equal(2, box.Count);
    }

    /// <summary>Stops on HttpRequestException when maxAttempts is 1.</summary>
    [Fact]
    public async Task StopsHttpWhenMaxAttemptsIsOneAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-http-stop");
        _ = await AsyncAssert.ThrowsAsync<HttpRequestException, int>(
            policy.ExecuteAsync(static (_, _) => ValueTask.FromException<int>(new HttpRequestException("boom")), 0, CancellationToken.None));
    }

    private static async Task HammerExecuteUntilDisposedAsync(CallPolicy policy, ManualResetEventSlim drained, ConcurrentQueue<string> faults, TaskCompletionSource readySignal)
    {
        while (!drained.IsSet)
        {
            try
            {
                _ = await policy.ExecuteAsync(static (_, _) => ValueTask.FromResult(1), 0, CancellationToken.None);
                _ = readySignal.TrySetResult();
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
                if (string.Equals(disposed.ObjectName, nameof(SemaphoreSlim), StringComparison.Ordinal))
                    faults.Enqueue(disposed.ToString());

                return;
            }
        }
    }

    private static Task[] StartHammerCallers(CallPolicy policy, ManualResetEventSlim drained, ConcurrentQueue<string> faults, int count, out Task[] callers)
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
