using System;
using System.Collections.Concurrent;
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

namespace Squirix.UnitTests.Cluster;

/// <summary>Covers client <see cref="CallPolicy" /> Map* failure classification paths.</summary>
[Immutable]
public sealed class CallPolicyTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>Rejects new calls after BeginDrain.</summary>
    [Test]
    public async Task BeginDrainRejectsNewCallsAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-drain");
        policy.BeginDrain();

        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(policy.ExecuteAsync(static (_, _) => ValueTask.FromResult(1), 0, CancellationToken.None));
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <summary>
    /// Dispose racing <see cref="CallPolicy.ExecuteAsync{TState,T}" /> must never surface an
    /// <see cref="ObjectDisposedException" /> raised from SemaphoreSlim internals: the
    /// claim-then-recheck ordering makes racing callers observe disposal through the policy's own
    /// post-enter check (or the drain gate) instead of a disposed concurrency semaphore.
    /// Mirrors the server-side regression test for the same race.
    /// </summary>
    [Test]
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

            // ReSharper disable once DisposeOnUsingVariable — intentional mid-race disposal: the test covers dispose landing inside the execute loop.
            await policy.DisposeAsync();
            drained.Set();

            foreach (var caller in callers)
                await caller;

            _ = await Assert.That(faults.TryPeek(out var fault)).IsFalse().Because($"SemaphoreSlim disposed fault escaped to a caller: {fault}");
        }
    }

    /// <summary>The operation budget expires on the clock its deadline was pushed with, with no real delay.</summary>
    [Test]
    public async Task BudgetExpiresOnPushClock()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var policy = new CallPolicy(TimeSpan.FromHours(2), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-budget-clock", timeProvider: clock);
        using var scope = RpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + TimeSpan.FromHours(1), clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var call = policy.ExecuteAsync(static (signal, ct) => BlockUntilCanceledAsync(signal, ct), entered, CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(Bound, TimeProvider.System, CancellationToken.None);
        var pending = !call.IsCompleted;
        clock.Advance(TimeSpan.FromHours(1));
        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
    }

    /// <summary>The deadline handed to gRPC is the remaining budget from the current time of the clock the deadline was pushed with.</summary>
    [Test]
    public async Task ForwardDeadlineFollowsBudget()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        using var scope = RpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + TimeSpan.FromHours(1), clock);
        clock.Advance(TimeSpan.FromMinutes(10));

        var expected = clock.GetUtcNow().UtcDateTime + TimeSpan.FromMinutes(50);

        _ = await Assert.That(RpcDeadlineContext.ForwardDeadlineUtc).IsEqualTo(expected);
    }

    /// <summary>The per-attempt timeout runs on the policy clock, with no real delay.</summary>
    [Test]
    public async Task AttemptTimeoutRunsOnPolicyClock()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var policy = new CallPolicy(TimeSpan.FromHours(1), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "c-attempt-clock", timeProvider: clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var call = policy.ExecuteAsync(static (signal, ct) => BlockUntilCanceledAsync(signal, ct), entered, CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(Bound, TimeProvider.System, CancellationToken.None);
        var pending = !call.IsCompleted;
        clock.Advance(TimeSpan.FromHours(1));
        _ = await AsyncAssert.ThrowsAnyAsync<RpcException, int>(new ValueTask<int>(call.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));

        _ = await Assert.That(pending).IsTrue();
    }

    /// <summary>Stops on non-retryable Rpc status.</summary>
    [Test]
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
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(box.Count).IsEqualTo(1);
    }

    /// <summary>Retries Unavailable RpcException.</summary>
    [Test]
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

        _ = await Assert.That(value).IsEqualTo(8);
        _ = await Assert.That(box.Count).IsEqualTo(2);
    }

    /// <summary>Rejects a call queued behind the concurrency gate when drain begins before execution.</summary>
    [Test]
    public async Task QueuedCallRejectedOnDrainAsync()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var policy = new CallPolicy(timeout, 1, TimeSpan.Zero, TimeSpan.Zero, 1, "c-drain-queue", clock);
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
        var queuedPending = !queued.IsCompleted;

        policy.BeginDrain();
        releaseFirst.SetResult();

        _ = await Assert.That(queuedPending).IsTrue();
        _ = await Assert.That(await first).IsEqualTo(1);
        var ex = await AsyncAssert.ThrowsAsync<RpcException, int>(queued);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <summary>Retries DeadlineExceeded RpcException.</summary>
    [Test]
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

        _ = await Assert.That(value).IsEqualTo(4);
        _ = await Assert.That(box.Count).IsEqualTo(2);
    }

    /// <summary>Retries HttpRequestException then succeeds.</summary>
    [Test]
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

        _ = await Assert.That(value).IsEqualTo(3);
        _ = await Assert.That(box.Count).IsEqualTo(2);
    }

    /// <summary>Stops on HttpRequestException when maxAttempts is 1.</summary>
    [Test]
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

    private static async ValueTask<int> BlockUntilCanceledAsync(TaskCompletionSource entered, CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        return 0;
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
