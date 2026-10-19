using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>The timed wait that reports a timeout as a result, not as an exception.</summary>
[Immutable]
public sealed class TimedWaitTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The wait ends with false when the delay elapses first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DelayEndsTheWaitWithFalse(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = WaitAsync(never.Task, TimeSpan.FromSeconds(1), clock, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>The wait ends with true when the awaited task completes before the delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletionEndsTheWaitWithTrue(CancellationToken cancellationToken)
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = WaitAsync(signal.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        signal.SetResult();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>An infinite delay waits for the task only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InfiniteDelayWaitsForTheTask(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = WaitAsync(signal.Task, Timeout.InfiniteTimeSpan, clock, cancellationToken);

        clock.Advance(TimedWait.MaxDelay);
        _ = await Assert.That(wait.IsCompleted).IsFalse();
        signal.SetResult();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>Canceling the token ends the wait by throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsTheWaitByThrowing(CancellationToken cancellationToken)
    {
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = WaitAsync(never.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token);

        await canceled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
    }

    private static Task<bool> WaitAsync(Task task, TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken) =>
        TimedWait.WaitAsync(
            task,
            static (awaited, token) => awaited.WaitAsync(token),
            static awaited => awaited.IsCompleted,
            delay,
            timeProvider,
            cancellationToken);
}
