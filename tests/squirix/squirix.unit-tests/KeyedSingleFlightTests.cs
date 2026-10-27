using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for per-key single-flight coordination.</summary>
[Immutable]
public sealed class KeyedSingleFlightTests
{
    /// <summary>Ensures a caller whose token is canceled stops waiting while a caller that joined it gets the result.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledOwnerLeavesJoinerWaiting(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var owner = new CancellationTokenSource();
        using var joiner = new CancellationTokenSource();

        var first = RunGatedAsync(flights, state, owner.Token);
        var second = RunGatedAsync(flights, state, joiner.Token);
        await owner.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(first));
        state.Gate.SetResult();

        _ = await Assert.That(await second.WaitAsync(cancellationToken)).IsEqualTo(7);
        _ = await Assert.That(error.CancellationToken).IsEqualTo(owner.Token);
        _ = await Assert.That(state.Executions).IsEqualTo(1);
        _ = await Assert.That(state.SharedTokenCanceled).IsFalse();
    }

    /// <summary>Ensures a joiner whose token is canceled stops waiting with its own token while the caller that started the flight gets the result.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledJoinerLeavesOwnerWaiting(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var owner = new CancellationTokenSource();
        using var joiner = new CancellationTokenSource();

        var first = RunGatedAsync(flights, state, owner.Token);
        var second = RunGatedAsync(flights, state, joiner.Token);
        await joiner.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(second));
        state.Gate.SetResult();

        _ = await Assert.That(await first.WaitAsync(cancellationToken)).IsEqualTo(7);
        _ = await Assert.That(error.CancellationToken).IsEqualTo(joiner.Token);
        _ = await Assert.That(state.SharedTokenCanceled).IsFalse();
    }

    /// <summary>Ensures the shared work is canceled once the last waiting caller has canceled, and sends no request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LastCallerToLeaveCancelsSharedWork(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var owner = new CancellationTokenSource();
        using var joiner = new CancellationTokenSource();

        var first = RunGatedAsync(flights, state, owner.Token);
        var second = RunGatedAsync(flights, state, joiner.Token);
        await owner.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(first));
        await joiner.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(second));
        await state.SharedTokenCanceledSignal.Task.WaitAsync(cancellationToken);
        state.Gate.SetResult();

        _ = await Assert.That(state.SharedTokenCanceled).IsTrue();
        _ = await Assert.That(state.Sends).IsEqualTo(0);
    }

    /// <summary>Ensures a joiner whose token cannot be canceled keeps the shared work running after the caller that started it left.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncancelableJoinerKeepsSharedWorkRunning(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var owner = new CancellationTokenSource();

        var first = RunGatedAsync(flights, state, owner.Token);
        var second = RunGatedAsync(flights, state, CancellationToken.None);
        await owner.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(first));
        state.Gate.SetResult();

        _ = await Assert.That(await second.WaitAsync(cancellationToken)).IsEqualTo(7);
        _ = await Assert.That(state.SharedTokenCanceled).IsFalse();
    }

    /// <summary>Ensures a caller that arrives after the flight sent its request reuses the produced value and sends its own request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateCallerReusesValueAndSendsOwnRequest(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var first = RunSendGatedAsync(5);
        await state.SendStarted.Task.WaitAsync(cancellationToken);
        var second = RunSendGatedAsync(9);
        state.Gate.SetResult();

        _ = await Assert.That(await first.WaitAsync(cancellationToken)).IsEqualTo(51);
        _ = await Assert.That(await second.WaitAsync(cancellationToken)).IsEqualTo(52);
        _ = await Assert.That(state.Executions).IsEqualTo(1);
        _ = await Assert.That(state.Sends).IsEqualTo(2);
        return;

        Task<int> RunSendGatedAsync(int value)
        {
            return flights.RunAsync(
                "k",
                (State: state, Value: value),
                static (callerState, _) =>
                {
                    callerState.State.IncrementExecutions();
                    return Task.FromResult(callerState.Value);
                },
                static async (callerState, produced, token) =>
                {
                    var send = callerState.State.IncrementSends();
                    _ = callerState.State.SendStarted.TrySetResult();
                    await callerState.State.Gate.Task.WaitAsync(token);
                    return (produced * 10) + send;
                },
                cancellationToken);
        }
    }

    /// <summary>Ensures a caller that arrives after the last caller left a producing flight starts a new flight instead of joining the canceled one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerAfterLastLeftStartsNewFlight(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();

        // Releasing the gate runs both produce steps to their end on this thread, so the assertions see any request of the abandoned flight.
        var state = new SingleFlightTestState(TaskCreationOptions.None);
        using var owner = new CancellationTokenSource();
        Func<SingleFlightTestState, CancellationToken, Task<int>> produceIgnoringCancel = static async (testState, _) =>
        {
            testState.IncrementExecutions();
            await testState.Gate.Task;
            return 7;
        };

        var first = RunAsync(flights, "k", state, produceIgnoringCancel, owner.Token);
        await owner.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(first));
        var next = RunAsync(flights, "k", state, produceIgnoringCancel, cancellationToken);
        state.Gate.SetResult();

        _ = await Assert.That(await next.WaitAsync(cancellationToken)).IsEqualTo(7);
        _ = await Assert.That(state.Executions).IsEqualTo(2);
        _ = await Assert.That(state.Sends).IsEqualTo(1);
    }

    /// <summary>Ensures the last caller to leave gets the cancel of its own token when a callback of the shared work throws.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaverKeepsOwnCancelOnCallbackFailure(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var owner = new CancellationTokenSource();

        var first = RunAsync(
            flights,
            "k",
            state,
            static async (testState, ct) =>
            {
                await using var registration = ct.Register(static () => throw new InvalidOperationException("callback failed"));
                await testState.WaitGateObservingAsync(ct);
                return 7;
            },
            owner.Token);
        await owner.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(first));
        await state.SharedTokenCanceledSignal.Task.WaitAsync(cancellationToken);
        state.Gate.SetResult();

        _ = await Assert.That(error.CancellationToken).IsEqualTo(owner.Token);
    }

    /// <summary>Ensures a canceled flight stays canceled and does not leave a stale flight for the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledFlightDoesNotPoisonKey(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        var flight = RunAsync(
            flights,
            "k",
            state,
            static (_, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(1);
            },
            canceled.Token);
        var next = await RunAsync(flights, "k", state, static (_, _) => Task.FromResult(3), cancellationToken);

        _ = await Assert.That(flight.IsCanceled).IsTrue();
        _ = await Assert.That(next).IsEqualTo(3);
    }

    /// <summary>Ensures a continuation of a completed flight starts a new flight rather than joining the finished one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ContinuationStartsNewFlight(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var first = RunAsync(
            flights,
            "k",
            state,
            static (testState, _) =>
            {
                testState.IncrementExecutions();
                return testState.Gate.Task.ContinueWith(static _ => 1, TaskScheduler.Default);
            },
            cancellationToken);
        var chained = first.ContinueWith(
            _ => RunAsync(
                flights,
                "k",
                state,
                static (testState, _) =>
                {
                    testState.IncrementExecutions();
                    return Task.FromResult(2);
                },
                cancellationToken),
            TaskScheduler.Default).Unwrap();
        state.Gate.SetResult();

        _ = await Assert.That(await first).IsEqualTo(1);
        _ = await Assert.That(await chained).IsEqualTo(2);
        _ = await Assert.That(state.Executions).IsEqualTo(2);
    }

    /// <summary>Ensures a joiner sharing a canceled flight observes cancellation rather than a fault.</summary>
    [Test]
    public async Task JoinerObservesCanceledFlightAsCanceled()
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();
        using var canceled = new CancellationTokenSource();

        var first = RunGatedWithTokenAsync();
        var joiner = RunGatedWithTokenAsync();
        await canceled.CancelAsync();
        state.Gate.SetResult();

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(joiner));
        _ = await Assert.That(first.IsCanceled).IsTrue();
        _ = await Assert.That(joiner.IsCanceled).IsTrue();
        _ = await Assert.That(joiner.IsFaulted).IsFalse();
        _ = await Assert.That(state.Executions).IsEqualTo(1);
        return;

        Task<int> RunGatedWithTokenAsync()
        {
            return RunAsync(
                flights,
                "k",
                state,
                static async (testState, ct) =>
                {
                    testState.IncrementExecutions();
                    await testState.Gate.Task;
                    ct.ThrowIfCancellationRequested();
                    return 1;
                },
                canceled.Token);
        }
    }

    /// <summary>Ensures concurrent callers observe the same factory exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PropagatesSameFailureToConcurrentCallers(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var first = RunFailingAsync();
        var second = RunFailingAsync();
        state.Gate.SetResult();

        InvalidOperationException? firstException = null;
        try
        {
            _ = await first;
        }
        catch (InvalidOperationException ex)
        {
            firstException = ex;
        }

        InvalidOperationException? secondException = null;
        try
        {
            _ = await second;
        }
        catch (InvalidOperationException ex)
        {
            secondException = ex;
        }

        _ = await Assert.That(firstException).IsNotNull();
        _ = await Assert.That(secondException).IsNotNull();
        _ = await Assert.That(state.Executions).IsEqualTo(1);
        return;

        Task<int> RunFailingAsync()
        {
            return RunAsync(
                flights,
                "k",
                state,
                static async (testState, ct) =>
                {
                    testState.IncrementExecutions();
                    await testState.Gate.Task.WaitAsync(ct);
                    throw new InvalidOperationException("factory failed");
                },
                cancellationToken);
        }
    }

    /// <summary>Ensures concurrent callers for one key share one execution.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunAsyncSharesOneExecutionForSameKey(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var first = RunOnceAsync();
        var second = RunOnceAsync();
        state.Gate.SetResult();

        _ = await Assert.That(state.Executions).IsEqualTo(1);
        _ = await Assert.That(await first).IsEqualTo(7);
        _ = await Assert.That(await second).IsEqualTo(7);
        return;

        Task<int> RunOnceAsync()
        {
            return RunAsync(
                flights,
                "k",
                state,
                static async (testState, ct) =>
                {
                    testState.IncrementExecutions();
                    await testState.Gate.Task.WaitAsync(ct);
                    return 7;
                },
                cancellationToken);
        }
    }

    /// <summary>Ensures a call after a synchronously completed flight runs the action again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SequentialCallRerunsAfterSyncCompletion(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var first = await RunAsync(
            flights,
            "k",
            state,
            static (testState, _) =>
            {
                testState.IncrementExecutions();
                return Task.FromResult(1);
            },
            cancellationToken);
        var second = await RunAsync(
            flights,
            "k",
            state,
            static (testState, _) =>
            {
                testState.IncrementExecutions();
                return Task.FromResult(2);
            },
            cancellationToken);

        _ = await Assert.That(first).IsEqualTo(1);
        _ = await Assert.That(second).IsEqualTo(2);
        _ = await Assert.That(state.Executions).IsEqualTo(2);
    }

    /// <summary>Ensures a synchronous action failure does not leave a stale flight for the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SynchronousFailureDoesNotPoisonKey(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int, int>();
        var state = new SingleFlightTestState();

        var failing = RunAsync(flights, "k", state, static (_, _) => throw new InvalidOperationException("sync failure"), cancellationToken);
        var error = await AsyncAssert.ThrowsAsync<InvalidOperationException, int>(new ValueTask<int>(failing));
        var next = await RunAsync(flights, "k", state, static (_, _) => Task.FromResult(7), cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo("sync failure");
        _ = await Assert.That(next).IsEqualTo(7);
    }

    private static Task<int> RunAsync(
        KeyedSingleFlight<int, int> flights,
        string key,
        SingleFlightTestState state,
        Func<SingleFlightTestState, CancellationToken, Task<int>> produce,
        CancellationToken cancellationToken) =>
        flights.RunAsync(key, state, produce, static (testState, produced, _) => testState.SendAsync(produced), cancellationToken);

    private static Task<int> RunGatedAsync(KeyedSingleFlight<int, int> flights, SingleFlightTestState state, CancellationToken cancellationToken) =>
        RunAsync(
            flights,
            "k",
            state,
            static async (testState, ct) =>
            {
                testState.IncrementExecutions();
                await testState.WaitGateObservingAsync(ct);
                return 7;
            },
            cancellationToken);

    private sealed class SingleFlightTestState
    {
        private int _executions;
        private int _sends;

        internal SingleFlightTestState(TaskCreationOptions gateOptions = TaskCreationOptions.RunContinuationsAsynchronously)
        {
            // A test may ask for inline continuations so releasing the gate runs the gated work to its end before the test goes on.
#pragma warning disable EPC32
            Gate = new TaskCompletionSource(gateOptions);
#pragma warning restore EPC32
        }

        internal int Executions => _executions;

        internal int Sends => _sends;

        internal bool SharedTokenCanceled => SharedTokenCanceledSignal.Task.IsCompleted;

        internal TaskCompletionSource Gate { get; }

        internal TaskCompletionSource SendStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource SharedTokenCanceledSignal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void IncrementExecutions() => _ = Interlocked.Increment(ref _executions);

        internal int IncrementSends() => Interlocked.Increment(ref _sends);

        internal Task<int> SendAsync(int produced)
        {
            _ = IncrementSends();
            return Task.FromResult(produced);
        }

        internal async Task WaitGateObservingAsync(CancellationToken sharedToken)
        {
            await using var registration = sharedToken.Register(() => SharedTokenCanceledSignal.TrySetResult());
            await Gate.Task;
            sharedToken.ThrowIfCancellationRequested();
        }
    }
}
