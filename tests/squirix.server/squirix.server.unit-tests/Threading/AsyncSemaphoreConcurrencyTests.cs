using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Covers permit conservation, queue order and cancellation/disposal races across semaphore paths.</summary>
public sealed class AsyncSemaphoreConcurrencyTests : ServerUnitTestBase
{
    private const int Workers = 16;
    private const int Iterations = 256;
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    /// <summary>Canceling the queued head preserves the order and capacity of the remaining waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledHeadPreservesRemainingOrder(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        using var cancel = new CancellationTokenSource();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        var first = semaphore.WaitAsync(cancel.Token);
        var last = semaphore.WaitAsync(cancellationToken);

        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(first);
        semaphore.Release();
        await last;
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
    }

    /// <summary>Canceling the queued middle preserves the order and capacity of the remaining waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledMiddlePreservesRemainingOrder(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        using var cancel = new CancellationTokenSource();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        var first = semaphore.WaitAsync(cancellationToken);
        var middle = semaphore.WaitAsync(cancel.Token);
        var last = semaphore.WaitAsync(cancellationToken);

        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(middle);
        semaphore.Release();
        await first;
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        _ = await Assert.That(last.IsCompleted).IsFalse();
        semaphore.Release();
        await last;
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
    }

    /// <summary>Canceling the queued tail preserves the order and capacity of the remaining waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledTailPreservesRemainingOrder(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        using var cancel = new CancellationTokenSource();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        var first = semaphore.WaitAsync(cancellationToken);
        var last = semaphore.WaitAsync(cancel.Token);

        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(last);
        semaphore.Release();
        await first;
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        semaphore.Release();
    }

    /// <summary>Concurrent waits, returns and optional queued cancellations preserve capacity and never over-admit.</summary>
    /// <param name="capacity">The permit capacity.</param>
    /// <param name="cancelQueued">Whether workers race their queued waits with cancellation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, false)]
    [Arguments(2, false)]
    [Arguments(8, false)]
    [Arguments(1, true)]
    [Arguments(2, true)]
    [Arguments(8, true)]
    public async Task ConcurrentReturnsPreserveEveryPermit(int capacity, bool cancelQueued, CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(capacity);
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(Guard);
        var run = new RaceRun(semaphore, capacity, cancelQueued, guard.Token);
        var workers = new Task[Workers];
        for (var i = 0; i < workers.Length; i++)
            workers[i] = run.ExecuteAsync();

        run.Start();
        await Task.WhenAll(workers).WaitAsync(Guard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(run.OverAdmitted).IsFalse();
        _ = await Assert.That(run.Active).IsEqualTo(0);
        _ = await Assert.That(run.Completed > 0).IsTrue();
        if (!cancelQueued)
            _ = await Assert.That(run.Completed).IsEqualTo(Workers * Iterations);

        for (var i = 0; i < capacity; i++)
            _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
        for (var i = 0; i < capacity; i++)
            semaphore.Release();
    }

    /// <summary>Returning one held permit while disposing never grants more than one queued wait and leaves none parked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeAndReturnCompleteEveryQueuedWait(CancellationToken cancellationToken)
    {
        using var semaphore = new AsyncSemaphore(1);
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        var queued = new Task[32];
        for (var i = 0; i < queued.Length; i++)
            queued[i] = semaphore.WaitAsync(CancellationToken.None).AsTask();

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispose = DisposeAsync();
        var release = ReleaseAsync();
        _ = start.TrySetResult();
        await Task.WhenAll(dispose, release).WaitAsync(Guard, TimeProvider.System, cancellationToken);
        var succeeded = 0;
        for (var i = 0; i < queued.Length; i++)
        {
            try
            {
                await queued[i].WaitAsync(Guard, TimeProvider.System, cancellationToken);
                succeeded++;
                semaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                _ = await Assert.That(queued[i].IsFaulted).IsTrue();
            }
        }

        _ = await Assert.That(succeeded <= 1).IsTrue();
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();

        async Task DisposeAsync()
        {
            await start.Task;
            semaphore.Dispose();
        }

        async Task ReleaseAsync()
        {
            await start.Task;
            semaphore.Release();
        }
    }

    /// <summary>The full positive integer capacity remains usable, and disposal outranks a pre-canceled wait.</summary>
    [Test]
    public async Task CapacityBitsAndDisposalAreIndependent()
    {
        using var semaphore = new AsyncSemaphore(int.MaxValue);
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        semaphore.Release();
        _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
        semaphore.Release();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        semaphore.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(semaphore.WaitAsync(canceled.Token));
        _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
    }

    /// <summary>A simultaneous empty-slot wait and return cannot lose the wakeup or create an extra permit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptySlotWaitRacingReturnPreservesPermit(CancellationToken cancellationToken)
    {
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(Guard);
        using var phases = new Barrier(3);
        var run = new QueueRaceRun(phases, guard.Token);
        var waiter = Task.Factory.StartNew(run.WaitActor, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var releaser = Task.Factory.StartNew(run.ReleaseActor, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        try
        {
            for (var i = 0; i < Iterations; i++)
            {
                using var semaphore = new AsyncSemaphore(1);
                _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
                run.Round(semaphore);
                await run.Waiting.WaitAsync(Guard, TimeProvider.System, guard.Token);
                _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
                semaphore.Release();
                _ = await Assert.That(semaphore.TryAcquire()).IsTrue();
                _ = await Assert.That(semaphore.TryAcquire()).IsFalse();
                semaphore.Release();
            }

            await Task.WhenAll(waiter, releaser).WaitAsync(Guard, TimeProvider.System, guard.Token);
        }
        finally
        {
            await guard.CancelAsync();
            await Task.WhenAll(waiter, releaser).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private sealed class QueueRaceRun
    {
        private readonly Barrier _phases;
        private readonly CancellationToken _token;
        private AsyncSemaphore? _current;
        private Task _waiting = Task.CompletedTask;

        internal QueueRaceRun(Barrier phases, CancellationToken token)
        {
            _phases = phases;
            _token = token;
        }

        internal Task Waiting => _waiting;

        internal void ReleaseActor()
        {
            for (var i = 0; i < Iterations; i++)
            {
                _phases.SignalAndWait(_token);
                Required(_current).Release();
                _phases.SignalAndWait(_token);
            }
        }

        internal void Round(AsyncSemaphore semaphore)
        {
            _current = semaphore;
            _waiting = Task.CompletedTask;
            _phases.SignalAndWait(_token);
            _phases.SignalAndWait(_token);
        }

        internal void WaitActor()
        {
            for (var i = 0; i < Iterations; i++)
            {
                _phases.SignalAndWait(_token);
                _waiting = Required(_current).WaitAsync(CancellationToken.None).AsTask();
                _phases.SignalAndWait(_token);
            }
        }

        private static AsyncSemaphore ThrowMissingSemaphore() => throw new InvalidOperationException("The round did not publish its semaphore.");

        private static AsyncSemaphore Required(AsyncSemaphore? semaphore) => semaphore ?? ThrowMissingSemaphore();
    }

    private sealed class RaceRun
    {
        private readonly bool _cancelQueued;
        private readonly int _capacity;
        private readonly AsyncSemaphore _semaphore;
        private readonly TaskCompletionSource _start = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationToken _token;
        private int _active;
        private int _completed;
        private int _overAdmitted;

        internal RaceRun(AsyncSemaphore semaphore, int capacity, bool cancelQueued, CancellationToken token)
        {
            _semaphore = semaphore;
            _capacity = capacity;
            _cancelQueued = cancelQueued;
            _token = token;
        }

        internal int Active => Volatile.Read(ref _active);

        internal int Completed => Volatile.Read(ref _completed);

        internal bool OverAdmitted => Volatile.Read(ref _overAdmitted) != 0;

        internal async Task ExecuteAsync()
        {
            await _start.Task.ConfigureAwait(false);
            for (var i = 0; i < Iterations; i++)
            {
                using var cancel = CancellationTokenSource.CreateLinkedTokenSource(_token);
                var waiting = _semaphore.WaitAsync(cancel.Token);
                if (_cancelQueued && !waiting.IsCompleted && (i & 1) == 0)
                    await cancel.CancelAsync().ConfigureAwait(false);

                try
                {
                    await waiting.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancel.IsCancellationRequested && !_token.IsCancellationRequested)
                {
                    continue;
                }

                if (Interlocked.Increment(ref _active) > _capacity)
                    _ = Interlocked.Exchange(ref _overAdmitted, 1);
                try
                {
                    if ((i & 7) == 0)
                        await Task.Yield();
                    _ = Interlocked.Increment(ref _completed);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _active);
                    _semaphore.Release();
                }
            }
        }

        internal void Start() => _ = _start.TrySetResult();
    }
}
