using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>A wedged ring observes the pipeline failure on its poll slices instead of parking producers forever.</summary>
[Immutable]
public sealed class BoundedJournalRingTests
{
    /// <summary>A full ring invokes the failure poll on slice expiry and keeps its slot accounting.</summary>
    [Test]
    public async Task SlicePollObservesPipelineFailure()
    {
        using var ring = new BoundedJournalRing(1);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        var probe = new PipelineFailureProbe();
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None, probe.Throw).AsTask());
        _ = await Assert.That(thrown).IsSameReferenceAs(probe.Reason);

        _ = await Assert.That(ring.TryDequeue(out var filler)).IsTrue();
        _ = await Assert.That(filler).IsNotNull();
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    /// <summary>NotifyWorkAvailable must not surface ObjectDisposedException after the ring is disposed.</summary>
    [Test]
    public async Task NotifySafeAfterDispose()
    {
        var ring = new BoundedJournalRing(4);
        ring.Dispose();

        Exception? thrown = null;
        try
        {
            ring.NotifyWorkAvailable();
        }
        catch (ObjectDisposedException ex)
        {
            thrown = ex;
        }

        _ = await Assert.That(thrown).IsNull();
    }

    /// <summary>A full ring drained completely must accept a full second round: no slot may leak.</summary>
    [Test]
    public async Task EnqueueDequeueKeepsSlots()
    {
        using var ring = new BoundedJournalRing(2);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellation.Token);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellation.Token);
        _ = await Assert.That(ring.TryDequeue(out var first)).IsTrue();
        _ = await Assert.That(first).IsNotNull();
        _ = await Assert.That(ring.TryDequeue(out var second)).IsTrue();
        _ = await Assert.That(second).IsNotNull();

        // A leaked slot would park one of these enqueues until the bounded token cancels and fails the test.
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellation.Token);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellation.Token);
        _ = await Assert.That(ring.TryDequeue(out var third)).IsTrue();
        _ = await Assert.That(third).IsNotNull();
        _ = await Assert.That(ring.TryDequeue(out var fourth)).IsTrue();
        _ = await Assert.That(fourth).IsNotNull();
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    /// <summary>Queued work makes the wait return without parking until the deadline.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WaitForWorkReturnsWhenWorkQueued(bool useHighResolutionTimer)
    {
        using var ring = new BoundedJournalRing(4, useHighResolutionTimer);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        var started = Stopwatch.GetTimestamp();
        ring.WaitForWork(60_000, CancellationToken.None);

        _ = await Assert.That(Stopwatch.GetElapsedTime(started)).IsLessThan(TimeSpan.FromSeconds(10));
    }

    /// <summary>A zero timeout never parks.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WaitForWorkZeroTimeoutReturns(bool useHighResolutionTimer)
    {
        using var ring = new BoundedJournalRing(4, useHighResolutionTimer);

        var started = Stopwatch.GetTimestamp();
        ring.WaitForWork(0, CancellationToken.None);

        _ = await Assert.That(Stopwatch.GetElapsedTime(started)).IsLessThan(TimeSpan.FromSeconds(10));
    }

    /// <summary>A notification during a long deadline wait wakes the waiter long before the deadline.</summary>
    /// <param name="useHighResolutionTimer">Whether the high-resolution timer is allowed.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task NotifyDuringDeadlineWaitWakes(bool useHighResolutionTimer, CancellationToken cancellationToken)
    {
        using var ring = new BoundedJournalRing(4, useHighResolutionTimer);
        using var parked = new ManualResetEventSlim(false);
        Thread? waiterThread = null;
        var waiter = Task.Factory.StartNew(
            () =>
            {
                Volatile.Write(ref waiterThread, Thread.CurrentThread);
                parked.Set();
                ring.WaitForWork(120_000, CancellationToken.None);
            },
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        _ = parked.Wait(TimeSpan.FromSeconds(10), cancellationToken);

        // A notification that lands before the wait is kept, so the notify is sent only once the waiter thread is blocked in it.
        await Volatile.Read(ref waiterThread).WaitUntilAsync(static t => IsBlocked(t), TimeSpan.FromSeconds(10), cancellationToken);
        ring.NotifyWorkAvailable();

        await waiter.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Disposing a ring whose journal thread already ran a deadline wait releases the timer and stays idempotent.</summary>
    [Test]
    public async Task DisposeAfterDeadlineWaitIsSafe()
    {
        var ring = new BoundedJournalRing(4);
        ring.WaitForWork(2, CancellationToken.None);

        ring.Dispose();
        ring.Dispose();

        ring.NotifyWorkAvailable();
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    private static bool IsBlocked(Thread? thread) => thread?.ThreadState.HasFlag(System.Threading.ThreadState.WaitSleepJoin) == true;

    private sealed class PipelineFailureProbe
    {
        internal InvalidOperationException Reason { get; } = new("pipeline failed");

        internal void Throw() => throw Reason;
    }
}
