using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for per-key single-flight coordination.</summary>
[Immutable]
public sealed class KeyedSingleFlightTests : UnitTestBase
{
    /// <summary>Ensures concurrent callers observe the same factory exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PropagatesSameFailureToConcurrentCallers(CancellationToken cancellationToken)
    {
        var flights = new KeyedSingleFlight<int>();
        var state = new SingleFlightTestState { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };

        var first = RunFailingAsync();
        var second = RunFailingAsync();
        await Task.Delay(30, cancellationToken);
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
        var state = new SingleFlightTestState { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };

        var first = RunOnceAsync();
        var second = RunOnceAsync();
        await Task.Delay(30, cancellationToken);
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

    private sealed class SingleFlightTestState
    {
        private int _executions;

        internal int Executions => _executions;

        internal required TaskCompletionSource Gate { get; init; }

        internal void IncrementExecutions() => _ = Interlocked.Increment(ref _executions);
    }
}
