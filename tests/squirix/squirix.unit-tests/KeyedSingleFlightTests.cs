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
public sealed class KeyedSingleFlightTests : TestBase
{
    /// <summary>Ensures concurrent callers observe the same factory exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PropagatesSameFailureToConcurrentCallers(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int>();
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
            return flights.RunAsync(
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
        var flights = new KeyedSingleFlight<int>();
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
            return flights.RunAsync(
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
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState();

        var first = await flights.RunAsync(
            "k",
            state,
            static (testState, _) =>
            {
                testState.IncrementExecutions();
                return Task.FromResult(1);
            },
            cancellationToken);
        var second = await flights.RunAsync(
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
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState();

        var failing = flights.RunAsync<SingleFlightTestState>("k", state, static (_, _) => throw new InvalidOperationException("sync failure"), cancellationToken);
        var error = await AsyncAssert.ThrowsAsync<InvalidOperationException, int>(new ValueTask<int>(failing));
        var next = await flights.RunAsync("k", state, static (_, _) => Task.FromResult(7), cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo("sync failure");
        _ = await Assert.That(next).IsEqualTo(7);
    }

    /// <summary>Ensures a canceled flight stays canceled and does not leave a stale flight for the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledFlightDoesNotPoisonKey(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        var flight = flights.RunAsync(
            "k",
            state,
            static (_, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(1);
            },
            canceled.Token);
        var next = await flights.RunAsync("k", state, static (_, _) => Task.FromResult(3), cancellationToken);

        _ = await Assert.That(flight.IsCanceled).IsTrue();
        _ = await Assert.That(next).IsEqualTo(3);
    }

    /// <summary>Ensures a joiner sharing a canceled flight observes cancellation rather than a fault.</summary>
    [Test]
    public async Task JoinerObservesCanceledFlightAsCanceled()
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState();
        using var canceled = new CancellationTokenSource();

        var first = RunGatedAsync();
        var joiner = RunGatedAsync();
        await canceled.CancelAsync();
        state.Gate.SetResult();

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(joiner));
        _ = await Assert.That(first.IsCanceled).IsTrue();
        _ = await Assert.That(joiner.IsCanceled).IsTrue();
        _ = await Assert.That(joiner.IsFaulted).IsFalse();
        _ = await Assert.That(state.Executions).IsEqualTo(1);
        return;

        Task<int> RunGatedAsync()
        {
            return flights.RunAsync(
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

    /// <summary>Ensures a continuation of a completed flight starts a new flight rather than joining the finished one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ContinuationStartsNewFlight(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState();

        var first = flights.RunAsync(
            "k",
            state,
            static (testState, _) =>
            {
                testState.IncrementExecutions();
                return testState.Gate.Task.ContinueWith(static _ => 1, TaskScheduler.Default);
            },
            cancellationToken);
        var chained = first.ContinueWith(
            _ => flights.RunAsync(
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

    private sealed class SingleFlightTestState
    {
        private int _executions;

        internal int Executions => _executions;

        internal TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void IncrementExecutions() => _ = Interlocked.Increment(ref _executions);
    }
}
