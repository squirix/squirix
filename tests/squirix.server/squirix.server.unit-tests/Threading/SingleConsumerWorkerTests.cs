using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the single-consumer worker lifecycle and ordering contract.</summary>
public sealed class SingleConsumerWorkerTests : ServerUnitTestBase
{
    /// <summary>Queued items are handled in FIFO order by one consumer, even with a pending backlog drained during disposal.</summary>
    [Fact]
    public async Task ProcessesItemsInFifoOrder()
    {
        var values = new List<int>();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSignal = new StrongBox<bool>(false);
        var pending = new List<Task>(3);

        var worker = new SingleConsumerWorker<int>(
            value =>
            {
                if (value == 1)
                {
                    _ = started.TrySetResult();

                    // Block the first handler so the remaining items form a genuine backlog.
                    lock (releaseSignal)
                    {
                        while (!releaseSignal.Value)
                            _ = Monitor.Wait(releaseSignal);
                    }
                }

                lock (values)
                    values.Add(value);
            },
            static (_, _) => { });

        try
        {
            // Queue the first item and wait until its handler is running and blocked, so the
            // remaining items accumulate as a genuine pending backlog in the worker queue.
            pending.Add(worker.EnqueueAsync(1));
            await started.Task;

            pending.Add(worker.EnqueueAsync(2));
            pending.Add(worker.EnqueueAsync(3));

            // Dispose waits for queued work to drain; release the blocked handler from a
            // LongRunning task so the worker processes the backlog while disposal is in progress.
            var disposeTask = Task.Factory.StartNew(worker.Dispose, DefaultCancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            lock (releaseSignal)
            {
                releaseSignal.Value = true;
                Monitor.Pulse(releaseSignal);
            }

            await disposeTask;
        }
        finally
        {
            worker.Dispose();
        }

        await Task.WhenAll(pending);
        Assert.Equal([1, 2, 3], values);
    }

    /// <summary>A failing item does not prevent later items from being handled.</summary>
    [Fact]
    public async Task IsolatesHandlerExceptions()
    {
        var handled = new List<int>();
        using var worker = new SingleConsumerWorker<int>(
            value =>
            {
                if (value == 1)
                    throw new InvalidOperationException("expected");

                handled.Add(value);
            },
            static (_, _) => { });

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => worker.EnqueueAsync(1));
        await worker.EnqueueAsync(2);

        Assert.Equal([2], handled);
    }

    /// <summary>A fire-and-forget Post routes a handler failure to onFault without throwing to the caller.</summary>
    [Fact]
    public async Task PostSurfacesHandlerFailureThroughOnFault()
    {
        var faulted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new StrongBox<Exception?>(null);
        using var worker = new SingleConsumerWorker<int>(
            static _ => throw new IOException("expected"),
            (_, ex) =>
            {
                failure.Value = ex;
                Complete(faulted);
            });

        worker.Post(1);

        await faulted.Task;
        _ = Assert.IsType<IOException>(failure.Value);
    }

    /// <summary>A Post after Dispose reports the failure through onFault and does not throw to the caller.</summary>
    [Fact]
    public void PostAfterDisposeReportsThroughOnFault()
    {
        var failure = new StrongBox<Exception?>(null);
        using var worker = new SingleConsumerWorker<int>(static _ => { }, (_, ex) => failure.Value = ex);

        // ReSharper disable once DisposeOnUsingVariable
        worker.Dispose();
        worker.Post(1);
        _ = Assert.IsType<ObjectDisposedException>(failure.Value);
    }

    /// <summary>Disposal drains items queued before completion.</summary>
    [Fact]
    public async Task DisposeDrainsQueuedItems()
    {
        var handled = 0;
        var worker = new SingleConsumerWorker<int>(_ => handled++, static (_, _) => { });
        var first = worker.EnqueueAsync(1);
        var second = worker.EnqueueAsync(2);
        var third = worker.EnqueueAsync(3);

        worker.Dispose();

        await Task.WhenAll(first, second, third);
        Assert.Equal(3, handled);
    }

    private static void Complete(TaskCompletionSource tcs) => _ = tcs.TrySetResult();
}
