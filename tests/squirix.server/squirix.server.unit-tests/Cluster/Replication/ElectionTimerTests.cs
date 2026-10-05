using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Deterministic election timeout driven by an injected time provider.</summary>
[Immutable]
public sealed class ElectionTimerTests : ServerUnitTestBase
{
    /// <summary>A dispose racing a running callback returns only after the callback finished, so no callback outlives the dispose.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWaitsForRunningCallback(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var timer = ElectionTimer.Create(3, null, time);
        _ = await Assert.That(timer).IsNotNull();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        timer.Start(() =>
        {
            entered.SetResult();
            release.Wait(cancellationToken);
        });

        // The fake clock runs the timer callback on the thread that advances it; both racing calls get their own thread.
        var tick = Task.Factory.StartNew(() => time.Advance(new ElectionTimerOptions().ElectionTimeout), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        bool disposedWhileRunning;
        Task dispose;
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            dispose = Task.Factory.StartNew(timer.Dispose, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            // The callback holds the gate until released, so the dispose cannot return however the threads are scheduled: once it
            // marked the timer disposed, what is left for it is taking the state lock, disposing the timer and the wait on the gate.
            await timer.WaitUntilAsync(static t => IsDisposed(t), TimeSpan.FromSeconds(10), cancellationToken);
            disposedWhileRunning = !await PendingProbe.StaysPendingAsync(dispose);
        }
        finally
        {
            release.Set();
        }

        await Task.WhenAll(tick, dispose).WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(disposedWhileRunning).IsFalse();
    }

    /// <summary>Re-arming from another thread does not wait for a running callback, so it cannot deadlock with a lock the callback takes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResetDoesNotWaitForRunningCallback(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var timer = ElectionTimer.Create(3, null, time);
        _ = await Assert.That(timer).IsNotNull();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        timer.Start(() =>
        {
            entered.SetResult();
            release.Wait(cancellationToken);
        });
        var tick = Task.Factory.StartNew(() => time.Advance(new ElectionTimerOptions().ElectionTimeout), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        bool callbackStillRunning;
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

            // The callback stays parked until after this reset returns: a reset that waited for it would time out here.
            var reset = Task.Factory.StartNew(timer.Reset, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            await reset.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            callbackStillRunning = !tick.IsCompleted;
        }
        finally
        {
            release.Set();
        }

        await tick.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(callbackStillRunning).IsTrue();
    }

    /// <summary>The callback may re-arm the timer, which then fires again, or dispose it, which then stays silent.</summary>
    [Test]
    public async Task CallbackMayResetOrDisposeTimer()
    {
        var time = new FakeTimeProvider();
        var timeout = new ElectionTimerOptions().ElectionTimeout;
        using var rearmed = ElectionTimer.Create(3, null, time);
        using var disposed = ElectionTimer.Create(3, null, time);
        _ = await Assert.That(rearmed).IsNotNull();
        _ = await Assert.That(disposed).IsNotNull();
        var rearmedFirings = 0;
        var disposedFirings = 0;
        rearmed.Start(() =>
        {
            if (++rearmedFirings == 1)
                rearmed.Reset();
        });
        disposed.Start(() =>
        {
            disposedFirings++;
            disposed.Dispose();
        });

        time.Advance(timeout);
        time.Advance(timeout);
        time.Advance(timeout);

        _ = await Assert.That((rearmedFirings, disposedFirings)).IsEqualTo((2, 1));
    }

    /// <summary>A dispose cancels an armed timeout, so its expiry invokes nothing.</summary>
    [Test]
    public async Task DisposeCancelsArmedTimeout()
    {
        var time = new FakeTimeProvider();
        var timer = ElectionTimer.Create(3, null, time);
        _ = await Assert.That(timer).IsNotNull();
        var firings = 0;
        timer.Start(() => firings++);

        timer.Dispose();
        time.Advance(new ElectionTimerOptions().ElectionTimeout);

        _ = await Assert.That(firings).IsEqualTo(0);
    }

    /// <summary>Starting a disposed timer throws instead of arming a new callback.</summary>
    [Test]
    public async Task StartAfterDisposeThrows()
    {
        var timer = ElectionTimer.Create(3, null, new FakeTimeProvider());
        _ = await Assert.That(timer).IsNotNull();
        timer.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(timer, static t => t.Start(static () => { }));
    }

    /// <summary>Advancing a fake time provider fires the election timeout without real waiting.</summary>
    [Test]
    public async Task UsesTimeProviderForDeterministicElection()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150) };
        using var timer = ElectionTimer.Create(3, options, time);
        _ = await Assert.That(timer).IsNotNull();

        var firings = 0;
        timer.Start(() => { firings++; });

        time.Advance(TimeSpan.FromMilliseconds(149));
        _ = await Assert.That(firings).IsEqualTo(0);

        time.Advance(TimeSpan.FromMilliseconds(1));
        _ = await Assert.That(firings).IsEqualTo(1);

        timer.Reset();
        time.Advance(TimeSpan.FromMilliseconds(149));
        _ = await Assert.That(firings).IsEqualTo(1);

        time.Advance(TimeSpan.FromMilliseconds(1));
        _ = await Assert.That(firings).IsEqualTo(2);
    }

    /// <summary>Reports whether the timer was marked disposed; a reset on a live timer re-arms the fake timer before the dispose reaches it, which is harmless as the clock is not advanced again.</summary>
    /// <param name="timer">The timer to probe.</param>
    /// <returns><see langword="true" /> once the timer is disposed.</returns>
    private static bool IsDisposed(ElectionTimer timer)
    {
        try
        {
            timer.Reset();
            return false;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }
}
