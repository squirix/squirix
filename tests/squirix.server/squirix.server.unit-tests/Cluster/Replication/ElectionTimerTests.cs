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
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        var dispose = Task.Factory.StartNew(timer.Dispose, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        // The callback holds the gate until released, so the dispose cannot have returned yet however the threads are scheduled.
        await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, cancellationToken);
        var disposedWhileRunning = dispose.IsCompleted;
        release.Set();
        await Task.WhenAll(tick, dispose).WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(disposedWhileRunning).IsFalse();
    }

    /// <summary>A timeout that elapses after a dispose invokes nothing.</summary>
    [Test]
    public async Task TickAfterDisposeInvokesNothing()
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
}
