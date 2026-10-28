using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>The timed wait that reports a timeout and a cancellation as results, not as exceptions.</summary>
[Immutable]
public sealed class TimedWaitTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The wait ends with the timed-out result when the delay elapses first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DelayEndsTheWaitAsTimedOut(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = WaitAsync(never.Task, TimeSpan.FromSeconds(1), clock, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.TimedOut);
    }

    /// <summary>The wait ends with the signaled result when the awaited task completes before the delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletionEndsTheWaitAsSignaled(CancellationToken cancellationToken)
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = WaitAsync(signal.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        signal.SetResult();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
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

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>Canceling the token ends the wait with the canceled result, without an exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsTheWaitAsCanceled(CancellationToken cancellationToken)
    {
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = WaitAsync(never.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token);

        await canceled.CancelAsync();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Canceled);
    }

    /// <summary>A condition that already holds wins over a canceled token.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeldConditionWinsOverCancellation(CancellationToken cancellationToken)
    {
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await canceled.CancelAsync();

        _ = await Assert.That(await WaitAsync(Task.CompletedTask, TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token)).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>A task that already completed ends the wait synchronously as signaled, even with a canceled token and a zero delay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedTaskEndsTheWaitSynchronously(CancellationToken cancellationToken)
    {
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await canceled.CancelAsync();

        var wait = TimedWait.WaitAsync(Task.CompletedTask, TimeSpan.Zero, new FakeTimeProvider(), canceled.Token);

        _ = await Assert.That(wait.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(await wait).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>A pending task ends the wait as timed out when the delay elapses first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingTaskTimesOut(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = TimedWait.WaitAsync(never.Task, TimeSpan.FromSeconds(1), clock, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.TimedOut);
    }

    /// <summary>A pending task ends the wait as canceled when the token cancels first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingTaskIsCanceledByTheToken(CancellationToken cancellationToken)
    {
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = TimedWait.WaitAsync(never.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), canceled.Token);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        await canceled.CancelAsync();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Canceled);
    }

    /// <summary>A pending task that completes before the delay ends the wait as signaled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingTaskCompletesBeforeTheDelay(CancellationToken cancellationToken)
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = TimedWait.WaitAsync(signal.Task, TimeSpan.FromHours(1), new FakeTimeProvider(), cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        signal.SetResult();

        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
    }

    private static Task<TimedWaitResult> WaitAsync(Task task, TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken) =>
        TimedWait.WaitAsync(
            task,
            static (awaited, token) => awaited.WaitAsync(token),
            static awaited => awaited.IsCompleted,
            delay,
            timeProvider,
            cancellationToken);
}
