using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Unit tests for per-key single-flight coordination.
/// </summary>
public sealed class KeyedSingleFlightTests
{
    /// <summary>
    /// Ensures concurrent callers observe the same factory exception.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task RunAsyncPropagatesSameFailureToConcurrentCallers()
    {
        var flights = new KeyedSingleFlight();
        var executions = 0;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = RunFailingAsync();
        var second = RunFailingAsync();
        await Task.Delay(30, TestContext.Current.CancellationToken);
        gate.SetResult();

        await AssertInvalidOperationAsync(first);
        await AssertInvalidOperationAsync(second);
        Assert.Equal(1, executions);
        return;

        Task<int> RunFailingAsync()
        {
            return flights.RunAsync<int>(
                "k",
                async ct =>
                {
                    _ = Interlocked.Increment(ref executions);
                    await gate.Task.WaitAsync(ct).ConfigureAwait(false);
                    throw new InvalidOperationException("factory failed");
                },
                TestContext.Current.CancellationToken);
        }

        static async Task AssertInvalidOperationAsync(Task<int> task)
        {
#pragma warning disable VSTHRD003
            // The test intentionally captures concurrent single-flight tasks before releasing the factory gate.
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await task.ConfigureAwait(false));
#pragma warning restore VSTHRD003
        }
    }

    /// <summary>
    /// Ensures concurrent callers for one key share one execution.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task RunAsyncSharesOneExecutionForSameKey()
    {
        var flights = new KeyedSingleFlight();
        var executions = 0;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = RunOnceAsync();
        var second = RunOnceAsync();
        await Task.Delay(30, TestContext.Current.CancellationToken);
        gate.SetResult();

        Assert.Equal(1, executions);
        Assert.Equal(7, await first);
        Assert.Equal(7, await second);
        return;

        Task<int> RunOnceAsync()
        {
            return flights.RunAsync(
                "k",
                async ct =>
                {
                    _ = Interlocked.Increment(ref executions);
                    await gate.Task.WaitAsync(ct).ConfigureAwait(false);
                    return 7;
                },
                TestContext.Current.CancellationToken);
        }
    }
}
