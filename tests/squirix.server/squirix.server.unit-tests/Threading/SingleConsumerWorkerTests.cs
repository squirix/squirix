using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the single-consumer worker lifecycle and ordering contract.</summary>
public sealed class SingleConsumerWorkerTests : ServerUnitTestBase
{
    /// <summary>Disposal drains a backlog queued behind a running handler in enqueue order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDrainsBacklogInOrder(CancellationToken cancellationToken)
    {
        const int backlog = 50;
        var order = new List<int>();
        using var handler = new BlockingHandler(cancellationToken);
        using var worker = new SingleConsumerWorker<int>(
            Bind.To<(BlockingHandler Handler, List<int> Order), Action<int>>(
                (handler, order),
                static s =>
                {
                    var (blocking, handled) = s;
                    return value =>
                    {
                        blocking.Handle(value);
                        lock (handled)
                            handled.Add(value);
                    };
                }),
            static (_, _) => { });
        var pending = new List<Task>(backlog) { worker.EnqueueAsync(1) };
        await handler.WaitUntilBlockedAsync();
        for (var value = 2; value <= backlog; value++)
            pending.Add(worker.EnqueueAsync(value));

        var disposing = Task.Factory.StartNew(worker.Dispose, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        handler.Release();
        await disposing.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        await Task.WhenAll(pending);
        var expected = new int[backlog];
        for (var index = 0; index < backlog; index++)
            expected[index] = index + 1;

        await SequenceAssert.EqualAsync(expected, order);
    }

    /// <summary>Disposal drains items queued before completion.</summary>
    [Test]
    public async Task DisposeDrainsQueuedItems()
    {
        var handled = 0;
        var worker = new SingleConsumerWorker<int>(_ => handled++, static (_, _) => { });
        var first = worker.EnqueueAsync(1);
        var second = worker.EnqueueAsync(2);
        var third = worker.EnqueueAsync(3);

        worker.Dispose();

        await Task.WhenAll(first, second, third);
        _ = await Assert.That(handled).IsEqualTo(3);
    }

    /// <summary>A stop requested by a handler returns at once instead of waiting for its own thread, and later items are refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeFromHandlerDoesNotDeadlock(CancellationToken cancellationToken)
    {
        var owner = new WorkerOwner();
        owner.Worker = new SingleConsumerWorker<int>(_ => owner.DisposeFromHandler(), static (_, _) => { });

        await owner.Worker.EnqueueAsync(1).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(owner.Worker.EnqueueAsync(2));
    }

    /// <summary>
    /// A dispose whose budget expires reports the timeout without throwing, even when the report itself fails, and a fire-and-forget item queued
    /// behind the blocked handler is refused through onFault.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeTimeoutReportsAndRefusesPosts(CancellationToken cancellationToken)
    {
        using var handler = new BlockingHandler(cancellationToken);
        var reports = new StrongBox<int>();
        var faults = new ConcurrentQueue<(int Item, Exception Fault)>();
        using var worker = new SingleConsumerWorker<int>(
            handler.Handle,
            (item, fault) => faults.Enqueue((item, fault)),
            TimeSpan.FromMilliseconds(100),
            () =>
            {
                _ = Interlocked.Increment(ref reports.Value);
                throw new InvalidOperationException("Reporting failed.");
            });
        worker.Post(1);
        worker.Post(2);
        await handler.WaitUntilBlockedAsync();

        var disposing = Task.Factory.StartNew(
            static state =>
            {
                if (state is IDisposable target)
                    target.Dispose();
            },
            worker,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        try
        {
            await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        }
        finally
        {
            handler.Release();
        }

        await faults.WaitUntilAsync(static queue => !queue.IsEmpty, cancellationToken);
        _ = await Assert.That(Volatile.Read(ref reports.Value)).IsEqualTo(1);
        _ = await Assert.That(faults.TryPeek(out var refused) && refused is { Item: 2, Fault: ObjectDisposedException }).IsTrue();
        _ = await Assert.That(handler.Handled).IsEqualTo(1);
    }

    /// <summary>An EnqueueAsync after Dispose returns a faulted task with ObjectDisposedException and does not throw to the caller.</summary>
    [Test]
    public async Task EnqueueAfterDisposeFaults()
    {
        var worker = new SingleConsumerWorker<int>(static _ => { }, static (_, _) => { });

        worker.Dispose();
        var task = worker.EnqueueAsync(1);

        _ = await Assert.That(task.IsCompletedSuccessfully).IsFalse();
        _ = await Assert.That(task.Exception?.InnerException).IsTypeOf<ObjectDisposedException>();
    }

    /// <summary>An enqueue racing a stop either runs its item or is refused with <see cref="ObjectDisposedException" />; no item is lost.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnqueueRacingStopRunsOrRefusesEveryItem(CancellationToken cancellationToken)
    {
        const int iterations = 300;
        for (var iteration = 0; iteration < iterations; iteration++)
            await RunRaceIterationAsync(cancellationToken);
    }

    /// <summary>A faulting onFault callback does not kill the worker thread, so later items still run.</summary>
    [Test]
    public async Task FaultingOnFaultCallbackDoesNotKillWorker()
    {
        var handled = new List<int>();
        using var worker = new SingleConsumerWorker<int>(
            value =>
            {
                if (value == 1)
                    throw new IOException("expected");

                handled.Add(value);
            },
            static (_, _) => throw new InvalidOperationException("onFault failed"));

        worker.Post(1);
        await worker.EnqueueAsync(2);

        await SequenceAssert.EqualAsync([2], handled);
    }

    /// <summary>Stopping an idle worker completes and raises no first-chance cancellation on the worker thread.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleStopRaisesNoFirstChanceCancel(CancellationToken cancellationToken)
    {
        const int idleDelayMilliseconds = 100;
        var workerThreadId = new StrongBox<int>();
        var cancellations = new StrongBox<int>();
        using var worker = new SingleConsumerWorker<int>(_ => Volatile.Write(ref workerThreadId.Value, Environment.CurrentManagedThreadId), static (_, _) => { });
        await worker.EnqueueAsync(1).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await Task.Delay(idleDelayMilliseconds, cancellationToken);

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        try
        {
            _ = await Assert.That(worker.TryStop(TimeSpan.FromSeconds(5))).IsTrue();
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
        }

        _ = await Assert.That(Volatile.Read(ref cancellations.Value)).IsEqualTo(0);
        return;

        void OnFirstChance(object? sender, FirstChanceExceptionEventArgs args)
        {
            if (args.Exception is OperationCanceledException && Environment.CurrentManagedThreadId == Volatile.Read(ref workerThreadId.Value))
                _ = Interlocked.Increment(ref cancellations.Value);
        }
    }

    /// <summary>A failing item does not prevent later items from being handled.</summary>
    [Test]
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

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(worker.EnqueueAsync(1));
        await worker.EnqueueAsync(2);

        await SequenceAssert.EqualAsync([2], handled);
    }

    /// <summary>A non-curated handler exception is isolated and does not kill the worker thread, so later items still run.</summary>
    [Test]
    public async Task IsolatesNonCuratedHandlerExceptions()
    {
        var handled = new List<int>();
        using var worker = new SingleConsumerWorker<int>(
            value =>
            {
                if (value == 1)
                    throw new ArgumentOutOfRangeException(nameof(value), "expected");

                handled.Add(value);
            },
            static (_, _) => { });

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException>(worker.EnqueueAsync(1));
        await worker.EnqueueAsync(2);

        await SequenceAssert.EqualAsync([2], handled);
    }

    /// <summary>A Post after Dispose reports the failure through onFault and does not throw to the caller.</summary>
    [Test]
    public async Task PostAfterDisposeReportsThroughOnFault()
    {
        var failure = new StrongBox<Exception?>(null);
        using var worker = new SingleConsumerWorker<int>(static _ => { }, (_, ex) => failure.Value = ex);

        // ReSharper disable once DisposeOnUsingVariable
        worker.Dispose();
        worker.Post(1);
        _ = await Assert.That(failure.Value).IsTypeOf<ObjectDisposedException>();
    }

    /// <summary>A fire-and-forget Post routes a handler failure to onFault without throwing to the caller.</summary>
    [Test]
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
        _ = await Assert.That(failure.Value).IsTypeOf<IOException>();
    }

    /// <summary>Queued items are handled in FIFO order by one consumer, even with a pending backlog drained during disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProcessesItemsInFifoOrder(CancellationToken cancellationToken)
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
            var disposeTask = Task.Factory.StartNew(worker.Dispose, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
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
        await SequenceAssert.EqualAsync([1, 2, 3], values);
    }

    /// <summary>
    /// A stop whose budget expires while a handler is blocked returns <see langword="false" />, and the item queued behind is refused instead of run
    /// once the handler returns.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopRefusesQueueAfterBudgetExpires(CancellationToken cancellationToken)
    {
        using var handler = new BlockingHandler(cancellationToken);
        using var worker = new SingleConsumerWorker<int>(handler.Handle, static (_, _) => { });
        var first = worker.EnqueueAsync(1);
        var second = worker.EnqueueAsync(2);
        await handler.WaitUntilBlockedAsync();

        var stopping = Task.Factory.StartNew(
            static state => state is SingleConsumerWorker<int> target && target.TryStop(TimeSpan.FromMilliseconds(100)),
            worker,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        bool stopped;
        try
        {
            stopped = await stopping.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        }
        finally
        {
            handler.Release();
        }

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(second);
        await first;
        _ = await Assert.That(stopped).IsFalse();
        _ = await Assert.That(handler.Handled).IsEqualTo(1);
    }

    private static void Complete(TaskCompletionSource tcs) => _ = tcs.TrySetResult();

    private static async Task RunRaceIterationAsync(CancellationToken cancellationToken)
    {
        const int itemsPerIteration = 20;
        const int waitSeconds = 10;
        var ran = 0;
        var refusedPosts = 0;
        using var worker = new SingleConsumerWorker<int>(_ => Interlocked.Increment(ref ran), (_, _) => Interlocked.Increment(ref refusedPosts));
        using var gate = new ManualResetEventSlim();
        var tasks = new Task[itemsPerIteration];
        await using var posts = ThreadRace.Start(
            (Worker: worker, Gate: gate, Tasks: tasks, Token: cancellationToken),
            static s =>
            {
                s.Gate.Wait(s.Token);
                for (var index = 0; index < itemsPerIteration; index++)
                {
                    s.Tasks[index] = s.Worker.EnqueueAsync(index);
                    s.Worker.Post(index);
                }
            },
            static s => s.Gate.Set(),
            CancellationToken.None);
        gate.Set();
        worker.Dispose();
        await posts.Completion.WaitAsync(TimeSpan.FromSeconds(waitSeconds), TimeProvider.System, cancellationToken);

        var refusedEnqueues = 0;
        foreach (var task in tasks)
        {
            try
            {
                await task.WaitAsync(TimeSpan.FromSeconds(waitSeconds), TimeProvider.System, cancellationToken);
            }
            catch (ObjectDisposedException)
            {
                refusedEnqueues++;
            }
        }

        // Stopping waits for the consumer to drain, so every accepted item has run and every refused one has been reported.
        _ = await Assert.That(Volatile.Read(ref ran) + Volatile.Read(ref refusedPosts) + refusedEnqueues).IsEqualTo(itemsPerIteration * 2);
    }

    /// <summary>Blocks the first item until released and counts the items the handler ran.</summary>
    private sealed class BlockingHandler : IDisposable
    {
        private readonly TaskCompletionSource _blocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationToken _cancellationToken;
        private readonly ManualResetEventSlim _release = new();
        private int _handled;

        internal BlockingHandler(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
        }

        internal int Handled => Volatile.Read(ref _handled);

        public void Dispose() => _release.Dispose();

        internal void Handle(int value)
        {
            if (value == 1)
            {
                _ = _blocked.TrySetResult();
                _release.Wait(_cancellationToken);
            }

            _ = Interlocked.Increment(ref _handled);
        }

        internal void Release() => _release.Set();

        internal Task WaitUntilBlockedAsync() => _blocked.Task.WaitAsync(_cancellationToken);
    }

    /// <summary>Owns a worker whose handler disposes it.</summary>
    private sealed class WorkerOwner
    {
        internal SingleConsumerWorker<int>? Worker { get; set; }

        internal void DisposeFromHandler() => Worker?.Dispose();
    }
}
