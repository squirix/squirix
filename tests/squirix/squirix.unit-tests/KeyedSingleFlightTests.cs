using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Unit tests for per-key single-flight coordination.</summary>
[Immutable]
public sealed class KeyedSingleFlightTests : UnitTestBase
{
    /// <summary>Ensures concurrent callers observe the same factory exception.</summary>
    [Fact]
    public async Task RunAsyncPropagatesSameFailureToConcurrentCallers()
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };

        var first = RunFailingAsync();
        var second = RunFailingAsync();
        await Task.Delay(30, DefaultCancellationToken);
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

        Assert.NotNull(firstException);
        Assert.NotNull(secondException);
        Assert.Equal(1, state.Executions);
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
                DefaultCancellationToken);
        }
    }

    /// <summary>Ensures concurrent callers for one key share one execution.</summary>
    [Fact]
    public async Task RunAsyncSharesOneExecutionForSameKey()
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };

        var first = RunOnceAsync();
        var second = RunOnceAsync();
        await Task.Delay(30, DefaultCancellationToken);
        state.Gate.SetResult();

        Assert.Equal(1, state.Executions);
        Assert.Equal(7, await first);
        Assert.Equal(7, await second);
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
                DefaultCancellationToken);
        }
    }

    private sealed class SingleFlightTestState
    {
        private int _executions;

        internal int Executions => _executions;

        internal required TaskCompletionSource Gate { get; init; }

        internal void IncrementExecutions() => _ = Interlocked.Increment(ref _executions);
    }
}
