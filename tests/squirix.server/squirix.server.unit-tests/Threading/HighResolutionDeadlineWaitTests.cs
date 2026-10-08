using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>The deadline wait wakes on the work signal at once, never returns early because of the timer, and falls back cleanly.</summary>
[Immutable]
public sealed class HighResolutionDeadlineWaitTests
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    /// <summary>A signal set before the wait is consumed without waiting for the deadline.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SignalBeforeWaitReturnsImmediately(bool useHighResolutionTimer)
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, useHighResolutionTimer);

        _ = signal.Set();
        var finiteStarted = Stopwatch.GetTimestamp();
        wait.Wait(60_000);
        var finiteElapsed = Stopwatch.GetElapsedTime(finiteStarted);

        _ = signal.Set();
        var infiniteStarted = Stopwatch.GetTimestamp();
        wait.Wait(Timeout.Infinite);
        var infiniteElapsed = Stopwatch.GetElapsedTime(infiniteStarted);

        _ = await Assert.That(finiteElapsed).IsLessThan(HangGuard);
        _ = await Assert.That(infiniteElapsed).IsLessThan(HangGuard);
    }

    /// <summary>A signal raised while a long finite wait is parked wakes the waiter long before the deadline.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SignalDuringFiniteWaitWakesWaiter(bool useHighResolutionTimer, CancellationToken cancellationToken)
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, useHighResolutionTimer);
        using var parked = new ManualResetEventSlim(false);
        var waiter = Task.Factory.StartNew(
            () =>
            {
                parked.Set();
                wait.Wait(120_000);
            },
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        _ = parked.Wait(HangGuard, cancellationToken);
        await Task.Delay(50, cancellationToken);
        _ = signal.Set();

        await waiter.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A finite wait never ends before its requested time; only a lower bound is asserted.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FiniteWaitNeverReturnsEarly(bool useHighResolutionTimer)
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, useHighResolutionTimer);

        var started = Stopwatch.GetTimestamp();
        wait.Wait(30);
        var elapsed = Stopwatch.GetElapsedTime(started);

        _ = await Assert.That(elapsed).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(28));
    }

    /// <summary>A timer that fired during an earlier wait stays out of a later infinite wait, which only the work signal ends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleTimerDoesNotWakeInfiniteWait(CancellationToken cancellationToken)
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, true);
        wait.Wait(2);

        var waiter = Task.Factory.StartNew(() => wait.Wait(Timeout.Infinite), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await Task.Delay(150, cancellationToken);
        _ = await Assert.That(waiter.IsCompleted).IsFalse();

        _ = signal.Set();
        await waiter.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A work signal ends a finite wait while its timer is still armed; the following infinite wait cancels that timer and parks until signalled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InfiniteWaitCancelsArmedTimer(CancellationToken cancellationToken)
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, true);

        _ = signal.Set();
        wait.Wait(5);

        // The armed 5 ms timer fires during this pause; a timer left in the wait set would end the infinite wait.
        var waiter = Task.Factory.StartNew(() => wait.Wait(Timeout.Infinite), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await Task.Delay(100, cancellationToken);
        _ = await Assert.That(waiter.IsCompleted).IsFalse();

        _ = signal.Set();
        await waiter.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>An earlier fired timer is re-armed by the next finite wait, so the later wait still honours its own deadline.</summary>
    [Test]
    public async Task ReArmedTimerIgnoresEarlierFire()
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, true);
        wait.Wait(2);

        var started = Stopwatch.GetTimestamp();
        wait.Wait(200);
        var elapsed = Stopwatch.GetElapsedTime(started);

        _ = await Assert.That(elapsed).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(190));
    }

    /// <summary>With the timer disabled the wait uses the work signal only and reports itself unavailable.</summary>
    [Test]
    public async Task FallbackModeUsesWorkSignalOnly()
    {
        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, false);

        wait.Wait(30);

        _ = await Assert.That(wait.IsHighResolutionActive).IsFalse();
        _ = await Assert.That(wait.IsHighResolutionUnavailable).IsTrue();
        _ = await Assert.That(signal.WaitOne(0)).IsFalse();

        _ = signal.Set();
        wait.Wait(60_000);
        _ = await Assert.That(signal.WaitOne(0)).IsFalse();
    }

    /// <summary>Disposal is idempotent, leaves the caller-owned work signal usable, and later waits fall back to the signal.</summary>
    [Test]
    public async Task DisposeReleasesTimerAndIsIdempotent()
    {
        using var signal = new AutoResetEvent(false);
        var wait = new HighResolutionDeadlineWait(signal, true);
        wait.Wait(2);

        wait.Dispose();
        wait.Dispose();

        _ = await Assert.That(wait.IsHighResolutionActive).IsFalse();
        _ = signal.Set();
        _ = await Assert.That(signal.WaitOne(0)).IsTrue();
        _ = signal.Set();
        wait.Wait(60_000);
    }

    /// <summary>Windows 10 version 1803 and later create the high-resolution timer on the first finite wait.</summary>
    [Test]
    public async Task TimerIsActiveOnSupportedWindows()
    {
        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 17134))
            return;

        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, true);
        _ = await Assert.That(wait.IsHighResolutionActive).IsFalse();

        wait.Wait(2);

        _ = await Assert.That(wait.IsHighResolutionActive).IsTrue();
        _ = await Assert.That(wait.IsHighResolutionUnavailable).IsFalse();
    }
}
